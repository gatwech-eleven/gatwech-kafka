import { nanoid } from "nanoid";
import { z } from "@gatwech/utils/zod";
import { createKafkaClient, initializeKafkaTopics } from '../../src/index';
import type { TopicConfig } from "../../src/config/topics/topic-config-schema";

// Define schema
const UserCreatedSchema = z.object({
  userId: z.string(),
  email: z.string().email(),
});

// Define topic
const userCreatedTopic: TopicConfig & { schema: typeof UserCreatedSchema } = {
  name: `e2e-users-created-${nanoid()}`,
  schema: UserCreatedSchema,
  retentionMs: 3600000,
  cleanupPolicy: "delete",
};

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const groupId = `e2e-group-${nanoid().slice(0, 6)}`;

  console.log(`Starting Typed Consumer E2E Test`);
  console.log(`   Topic: ${userCreatedTopic.name}`);
  console.log(`   Group: ${groupId}`);

  const kafka = createKafkaClient({
    config: {
      clientId: "e2e-typed-client",
      brokers: [bootstrap],
      metricsEnabled: false,
      connectWithRetries: true,
      maxConnectAttemptTimeMs: 60_000,
    },
  });

  await kafka.connect();
  console.log("Kafka connected");

  // Ensure topic exists
  const admin = kafka.getAdmin();
  await initializeKafkaTopics(admin, {
    partitions: 1,
    replicationFactor: 1,
    topics: {
      userCreated: userCreatedTopic,
    },
  });
  console.log("Topic ensured");

  // Setup consumer promise
  const consumedPromise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Timeout waiting for message")), 30000);

    kafka.consume(
      groupId,
      userCreatedTopic,
      async (payload: { userId: string; email: string }) => {
        console.log("📥 Received payload:", payload);
        try {
          if (payload.userId === "user-123" && payload.email === "test@example.com") {
            console.log("Payload verified");
            clearTimeout(timeout);
            resolve();
          } else {
            console.warn("Unexpected payload content");
          }
        } catch (err) {
          reject(err);
        }
      }
    ).catch(reject);
  });

  // Wait a bit for consumer to join
  await new Promise((r) => setTimeout(r, 5000));

  // Produce message
  const producer = kafka.getProducer();
  await producer.send({
    topic: userCreatedTopic.name,
    messages: [
      {
        key: "user-123",
        value: JSON.stringify({
          userId: "user-123",
          email: "test@example.com",
        }),
      },
    ],
  });
  console.log("📤 Message produced");

  // Wait for consumption
  await consumedPromise;

  await kafka.disconnect();
  console.log("✨ E2E Test Passed");
}

main().catch((err) => {
  console.error("E2E Test Failed:", err);
  process.exit(1);
});
