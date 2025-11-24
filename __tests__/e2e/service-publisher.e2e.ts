import { nanoid } from "nanoid";
import { KafkaClient } from '../../src/client/core/kafka.ts';
import { createServicePublisherFactory } from '../../src/client/producers/service-publisher-factory.ts';
import { z } from "@gatwech/utils/zod";
import crypto from "node:crypto";
import assert from "node:assert";
import { initializeKafkaTopics } from "../../src/config/ensure-topics.ts";

// Schema for service-level event: timestamp epoch(ms) provided by factory if absent
const ServiceEventSchema = z.object({
  userId: z.string(),
  action: z.string(),
  timestamp: z.number().int().nonnegative(),
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
  const topic = process.env.SERVICE_E2E_TOPIC || `service-e2e-${Date.now()}`;
  const serviceSource = process.env.SERVICE_SOURCE || "service-e2e-source";
  console.log(
    `🧪 Service Publisher E2E: bootstrap=${bootstrap}, topic=${topic}`
  );

  // Allow overriding connect timeout via env var for CI workflows.
  // On GitHub Actions default to a shorter timeout to avoid long blocked jobs.
  const defaultMaxConnectMs = process.env.GITHUB_ACTIONS ? 30_000 : 180_000;
  const maxConnectAttemptTimeMs = process.env.SERVICE_E2E_MAX_CONNECT_MS
    ? Number(process.env.SERVICE_E2E_MAX_CONNECT_MS)
    : defaultMaxConnectMs;

  const client = new KafkaClient("service-e2e-client", [bootstrap], {
    metricsEnabled: false,
    connectWithRetries: true,
    maxConnectAttemptTimeMs,
    connectPollIntervalMs: 1000,
  });
  await client.connect();

  // Ensure topic exists using library ensure-topics (with dynamic topic injection)
  try {
    const admin = client.getAdmin();
    await initializeKafkaTopics(admin, {
      partitions: 3,
      replicationFactor: 1,
      waitForLeadersMs: 15_000,
      topics: {
        serviceTopic: {
          name: topic,
          // basic retention & delete policy suitable for tests
          retentionMs: 24 * 60 * 60 * 1000,
          cleanupPolicy: "delete",
          partitions: 3,
          replicationFactor: 1,
        } as any,
      },
    });
    console.log(`🪶 Topic ensured: ${topic}`);
  } catch (err: any) {
    console.warn("⚠️ Topic creation skipped:", err?.message || err);
  }

  // Build service-level publisher factory
  const producer = client.getProducer();
  const defineEvent = createServicePublisherFactory(producer, serviceSource);
  const getPartitionKey = (p: z.infer<typeof ServiceEventSchema>) => p.userId;
  const publishUserEvent = defineEvent(
    topic,
    ServiceEventSchema,
    getPartitionKey
  );

  // Compute schema hash for observability
  const schemaHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(Object.keys(ServiceEventSchema.shape)))
    .digest("hex");

  // Start consumer
  const groupId = `service-e2e-consumer-${nanoid().slice(0, 8)}`;
  const consumer = client.createConsumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const payloadBase = { userId: "u123", action: "login" };
  const start = Date.now();

  const roundtripPromise = new Promise<number>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Timeout waiting for consumed event")),
      15000
    );

    consumer
      .run({
        eachMessage: async ({ message }) => {
          try {
            const valueStr = message.value?.toString();
            const parsed = JSON.parse(valueStr || "{}");
            console.log("✅ Consumed:", parsed);

            // Validate headers
            const headers = message.headers || {};
            const receivedCorrelation = headers["correlation-id"]?.toString();
            const receivedSource = headers["source"]?.toString();
            const receivedSchemaHash = headers["schema-hash"]?.toString();
            const receivedTimestampIso = headers["timestamp"]?.toString();

            if (!receivedCorrelation) {
              return reject(new Error("Missing correlation-id header"));
            }
            if (receivedSource !== serviceSource) {
              return reject(new Error(`Unexpected source: ${receivedSource}`));
            }
            if (
              !receivedTimestampIso ||
              isNaN(Date.parse(receivedTimestampIso))
            ) {
              return reject(new Error("Invalid or missing timestamp header"));
            }
            if (receivedSchemaHash && receivedSchemaHash !== schemaHash) {
              return reject(new Error("Schema hash mismatch"));
            }

            // Validate payload content (timestamp injected, metadata enrichment)
            assert.strictEqual(parsed.userId, payloadBase.userId);
            assert.strictEqual(parsed.action, payloadBase.action);
            assert.ok(
              typeof parsed.timestamp === "number",
              "timestamp should be number epoch ms"
            );
            assert.ok(
              parsed.metadata?.correlationId,
              "metadata.correlationId injected"
            );
            assert.strictEqual(parsed.metadata?.source, serviceSource);

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

  // Publish (metadata will be enriched by factory; include schemaHash so it propagates)
  const toPublish = {
    ...payloadBase,
    metadata: { schemaHash },
  } as any;

  await publishUserEvent(toPublish);
  console.log("🚀 Published:", toPublish);

  const latencyMs = await roundtripPromise;
  console.log(`🎉 Service Publisher E2E Success: roundtrip ${latencyMs}ms`);

  // Cleanup
  await consumer.disconnect();
  await client.disconnect();
}

main().catch((err) => {
  console.error("❌ Service Publisher E2E Failed:", err);
  process.exit(1);
});
