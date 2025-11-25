import { nanoid } from "nanoid";
import { KafkaClient } from '../../src/client/core/kafka.ts';
import { createEventPublisher } from '../../src/client/producers/event.factory.ts';
import { z } from "@gatwech/utils/zod";
import assert from "node:assert";
import crypto from "node:crypto";
import clientProm from "prom-client";
import { initializeKafkaTopics } from "../../src/config/initialize.topics.ts";

const e2eSchema = z.object({
  hello: z.string(),
  ts: z.iso.datetime(),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
      schemaHash: z.string().optional(),
    })
    .optional(),
});

async function waitForBroker(client: KafkaClient, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const admin = client.getAdmin();
      await admin.connect();
      const info = await admin.describeCluster();
      await admin.disconnect();
      if (info.controller) return;
    } catch (_) {
      // ignore and retry
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("Broker readiness timeout");
}

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = process.env.E2E_TOPIC || `e2e-test-${Date.now()}`;
  console.log(`E2E Kafka test: bootstrap=${bootstrap}, topic=${topic}`);

  const client = new KafkaClient("e2e-client", [bootstrap], {
    metricsEnabled: false,
    maxConnectionRetries: 1, // Fail fast for E2E
    resilience: {
      enabled: false, /* Disable for E2E - fail fast if Kafka unavailable */
    },
  });
  await client.connect();
  await waitForBroker(client, 30000);

  const producer = client.getProducer();

  // Ensure topic using robust helper (retries + leader wait)
  try {
    const admin = client.getAdmin();
    await initializeKafkaTopics(admin, {
      partitions: 3,
      replicationFactor: 1,
      waitForLeadersMs: 15_000,
      topics: {
        testTopic: {
          name: topic,
          retentionMs: 24 * 60 * 60 * 1000,
          cleanupPolicy: "delete",
        } as any,
      },
    });
    console.log(`Topic ensured: ${topic}`);
  } catch (err: any) {
    console.warn("Topic creation skipped:", err?.message || err);
  }

  // create publisher using producer instance
  const publisher = createEventPublisher(
    producer,
    topic,
    e2eSchema,
    (p) => `key-${(p as any).hello}`,
    "e2e-test-source"
  );

  // prometheus metrics
  const register = new clientProm.Registry();
  clientProm.collectDefaultMetrics({ register });
  const hist = new clientProm.Histogram({
    name: "kafka_e2e_roundtrip_ms",
    help: "E2E roundtrip latency in ms",
    buckets: [10, 50, 100, 250, 500, 1000, 2000],
    registers: [register],
  });

  // Start consumer
  const groupId = `e2e-consumer-${nanoid().slice(0, 8)}`;
  const consumer = client.createConsumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const payload = { hello: "world", ts: new Date().toISOString() };
  const schemaHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(e2eSchema.shape))
    .digest("hex");

  const start = Date.now();

  const messagePromise = new Promise<number>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Timeout waiting for message")),
      15000
    );
    consumer
      .run({
        eachMessage: async ({ message }) => {
          try {
            const value = message.value?.toString();
            const parsed = JSON.parse(value || "{}");
            console.log("Consumed:", parsed);

            // validate schema hash and metadata
            const headers = message.headers || {};
            const receivedSchemaHash = headers["schema-hash"]?.toString();
            const receivedCorrelation = headers["correlation-id"]?.toString();
            const receivedSource = headers["source"]?.toString();

            if (!receivedCorrelation) {
              return reject(new Error("Missing correlation-id header"));
            }
            if (receivedSource !== "e2e-test-source") {
              return reject(new Error(`Unexpected source: ${receivedSource}`));
            }
            if (receivedSchemaHash && receivedSchemaHash !== schemaHash) {
              return reject(new Error("Schema hash mismatch"));
            }

            // ensure payload matches exactly
            assert.deepStrictEqual(parsed.hello, payload.hello);
            clearTimeout(timeout);
            resolve(Date.now() - start);
          } catch (e) {
            clearTimeout(timeout);
            reject(e);
          }
        },
      })
      .catch(reject);
  });

  // publish with schema-hash in metadata so publisher attaches it to headers
  const payloadWithMeta = {
    ...payload,
    metadata: { source: "e2e-test-source", schemaHash },
  } as any;

  await publisher(payloadWithMeta as any);
  console.log("Produced:", payloadWithMeta);

  const latency = await messagePromise;
  hist.observe(latency);
  console.log(`E2E Success: roundtrip in ${latency}ms`);

  // print prometheus metrics for quick inspection
  const metrics = await register.metrics();
  console.log("--- Prometheus metrics ---\n", metrics);

  // Cleanup
  await consumer.disconnect();
  await client.disconnect();
}

main().catch((e) => {
  console.error("E2E Failed:", e);
  process.exit(1);
});
