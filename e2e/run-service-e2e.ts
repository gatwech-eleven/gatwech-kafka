import { nanoid } from "nanoid";
import { KafkaClient } from "../src/client/kafka.ts";
import { createServicePublisherFactory } from "../src/client/service-publisher-factory.ts";
import { z } from "@gatwech/utils/zod";
import crypto from "node:crypto";
import assert from "node:assert";
import type { Admin } from "kafkajs";

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

async function waitForBroker(client: KafkaClient, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const admin = client.getAdmin();
      await admin.connect();
      const info = await admin.describeCluster();
      await admin.disconnect();
      if (info.controller) return; // broker ready
    } catch (_) {
      // ignore
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error("Broker readiness timeout");
}

// Ensure a topic exists with the given partition count, retrying on transient errors
async function ensureTopicExists(
  admin: Admin,
  topic: string,
  partitions = 3,
  timeoutMs = 20000
) {
  const start = Date.now();
  let attempt = 0;
  // Keep a single admin connection open for the whole ensure flow
  await admin.connect();
  try {
    while (Date.now() - start < timeoutMs) {
      attempt++;
      try {
        // Check if topic already exists using listTopics() to avoid throwing on unknown
        const topics = await admin.listTopics();
        const exists = topics.includes(topic);
        if (!exists) {
          await admin.createTopics({
            topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
            waitForLeaders: true,
          });
        }

        // Wait for leaders to be elected for all partitions (defensive)
        const deadline = Date.now() + 10_000;
        while (Date.now() < deadline) {
          try {
            const md2 = await admin.fetchTopicMetadata({ topics: [topic] });
            const t = md2.topics.find((x) => x.name === topic);
            if (
              t &&
              t.partitions.length >= partitions &&
              t.partitions.every((p) => p.leader !== -1)
            ) {
              return;
            }
          } catch (_) {
            // if metadata fetch races with creation, just retry
          }
          await new Promise((r) => setTimeout(r, 300));
        }
        // If we exit the inner loop without leaders, try again
        throw new Error("leaders not ready yet");
      } catch (e: any) {
        const msg = e?.message || String(e);
        // Ignore benign races
        if (/already exists/i.test(msg)) return;
        // Transient connection/state errors: backoff and retry
        const delay = Math.min(250 * 2 ** attempt, 2000);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
    }
    throw new Error(`ensureTopicExists timeout for ${topic}`);
  } finally {
    await admin.disconnect();
  }
}

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = process.env.SERVICE_E2E_TOPIC || `service-e2e-${Date.now()}`;
  const serviceSource = process.env.SERVICE_SOURCE || "service-e2e-source";
  console.log(
    `🧪 Service Publisher E2E: bootstrap=${bootstrap}, topic=${topic}`
  );

  const client = new KafkaClient("service-e2e-client", [bootstrap], {
    metricsEnabled: false,
  });
  await client.connect();
  await waitForBroker(client, 30000);

  // Ensure topic exists (best effort)
  try {
    const admin = client.getAdmin();
    await ensureTopicExists(admin, topic, 3, 20000);
    console.log(`🪶 Topic ensured: ${topic}`);
  } catch (err: any) {
    console.warn("⚠️ Topic creation skipped:", err?.message || err);
  }

  // Build service-level publisher factory
  const producer = client.getProducer();
  const defineEvent = createServicePublisherFactory(producer, serviceSource);
  const keyDeriver = (p: z.infer<typeof ServiceEventSchema>) => p.userId;
  const publishUserEvent = defineEvent(topic, ServiceEventSchema, keyDeriver);

  // Compute schema hash for observability
  const schemaHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(Object.keys(ServiceEventSchema.shape)))
    .digest("hex");

  // Start consumer
  const groupId = `service-e2e-consumer-${nanoid().slice(0, 8)}`;
  const consumer = await client.createConsumer(groupId, { groupId });
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
