import { nanoid } from "nanoid";
import { KafkaClient } from "../src/client/kafka-client";
import { createEventPublisher } from "../src/client/event.factory";
import { z } from "@payvo/utils/zod";
import assert from "node:assert";
import crypto from "node:crypto";
import clientProm from "prom-client";

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

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = process.env.E2E_TOPIC || `e2e-test-${Date.now()}`;
  console.log(`🧪 E2E Kafka test: bootstrap=${bootstrap}, topic=${topic}`);

  const client = new KafkaClient("e2e-client", [bootstrap], {
    metricsEnabled: false,
  });
  await client.connect();

  const producer = client.getProducer();

  // create topic with multiple partitions (best-effort)
  try {
    const admin = client.getAdmin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
      waitForLeaders: true,
    });
    await admin.disconnect();
    console.log(`🪶 Topic ensured: ${topic}`);
  } catch (err: any) {
    console.warn("⚠️ Topic creation skipped:", err?.message || err);
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
  const consumer = await client.createConsumer(groupId, { groupId });
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
            console.log("✅ Consumed:", parsed);

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
  console.log("🚀 Produced:", payloadWithMeta);

  const latency = await messagePromise;
  hist.observe(latency);
  console.log(`🎉 E2E Success: roundtrip in ${latency}ms`);

  // print prometheus metrics for quick inspection
  const metrics = await register.metrics();
  console.log("--- Prometheus metrics ---\n", metrics);

  // Cleanup
  await consumer.disconnect();
  await client.disconnect();
}

main().catch((e) => {
  console.error("❌ E2E Failed:", e);
  process.exit(1);
});
