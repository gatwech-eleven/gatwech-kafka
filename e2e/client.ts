import {
  createKafkaClient,
  topicProvisioningExtension,
  healthCheckExtension,
  createTopicConfig,
} from "../src";
import { z } from "@gatwech/utils/zod";
import { randomUUID } from "crypto";

const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP || "localhost:9092";

// Define test topics
const TestTopics = createTopicConfig({
  MODULAR_TEST: {
    name: `modular.test.${randomUUID()}`,
    schema: z.object({ id: z.string(), timestamp: z.number() }),
    partitions: 1,
    cleanupPolicy: "delete",
  },
});

async function run() {
  console.log("🧪 Starting Modular Client E2E Test...");

  // Initialize client with extensions
  const client = createKafkaClient({
    config: {
      clientId: "modular-e2e-test",
      brokers: [KAFKA_BOOTSTRAP],
      logLevel: 1, // ERROR
    },
    extensions: [
      topicProvisioningExtension(TestTopics, {
        partitions: 1,
        replicationFactor: 1,
      }),
      healthCheckExtension(),
    ],
  });

  try {
    // 1. Connect
    console.log("Connecting...");
    await client.connect();
    console.log("✅ Connected");

    // 2. Health Check
    console.log("Checking health...");
    const isHealthy = await client.extensions.healthCheck.healthCheck();
    if (!isHealthy) throw new Error("Health check failed");
    console.log("✅ Health check passed");

    // 3. Provision Topics
    console.log("Provisioning topics...");
    await client.extensions.topicProvisioning.provisionTopics();
    console.log("✅ Topics provisioned");

    // 4. Produce Message
    const topic = TestTopics.MODULAR_TEST.name;
    const message = { id: randomUUID(), timestamp: Date.now() };
    
    console.log(`Producing message to ${topic}...`);
    await client.sendMessage(topic, {
      key: message.id,
      value: message,
    });
    console.log("✅ Message produced");

    // 5. Consume Message (using base client consumer)
    console.log("Consuming message...");
    const groupId = `test-group-${randomUUID()}`;
    const consumer = await client.createConsumer(groupId, { groupId });
    
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    const received = await new Promise<any>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }) => {
          if (message.value) {
            resolve(JSON.parse(message.value.toString()));
          }
        },
      });
    });

    if (received.id === message.id) {
      console.log("✅ Message consumed and verified");
    } else {
      throw new Error("Message mismatch");
    }

    await consumer.disconnect();

  } catch (error) {
    console.error("❌ Test failed:", error);
    process.exit(1);
  } finally {
    await client.disconnect();
    console.log("Test finished");
  }
}

run();
