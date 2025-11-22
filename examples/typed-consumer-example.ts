/**
 * Example: Type-Safe Consumer with Generic Payload Inference
 * 
 * This example demonstrates how consumeWithRetryAndDLQ automatically infers
 * the payload type from the topic's Zod schema, providing compile-time type safety.
 */

import { z } from "@gatwech/utils/zod";
import { createKafkaClient } from "../src/client/core/create-client";
import { consumeWithRetryAndDLQ } from "../src/client/consumers/consume-with-dlq";
import type { TopicConfig } from "../src/config/topics/topic-config-schema";

// Define schemas for our events
const UserCreatedSchema = z.object({
  userId: z.string().uuid(),
  email: z.string().email(),
  name: z.string(),
  timestamp: z.number(),
});

const OrderPlacedSchema = z.object({
  orderId: z.string(),
  userId: z.string().uuid(),
  items: z.array(z.object({
    productId: z.string(),
    quantity: z.number().positive(),
    price: z.number().positive(),
  })),
  total: z.number().positive(),
  timestamp: z.number(),
});

// Define topics with schemas
const userCreatedTopic: TopicConfig & { schema: typeof UserCreatedSchema } = {
  name: "users.created",
  schema: UserCreatedSchema,
  retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
  cleanupPolicy: "delete",
};

const orderPlacedTopic: TopicConfig & { schema: typeof OrderPlacedSchema } = {
  name: "orders.placed",
  schema: OrderPlacedSchema,
  retentionMs: 30 * 24 * 60 * 60 * 1000, // 30 days
  cleanupPolicy: "delete",
};

// Initialize Kafka client
const kafka = createKafkaClient({
  config: {
    clientId: "typed-consumer-example",
    brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
  },
});

async function main() {
  await kafka.connect();
  console.log("✅ Kafka connected");

  // Example 1: Consumer with typed payload (UserCreated)
  // The handler automatically receives the correct type!
  await consumeWithRetryAndDLQ(
    kafka,
    "user-service-group",
    userCreatedTopic,
    async (payload, ctx) => {
      // TypeScript knows payload is: { userId: string; email: string; name: string; timestamp: number }
      // You get full autocomplete and type checking!
      console.log(`New user created: ${payload.name} (${payload.email})`);
      console.log(`User ID: ${payload.userId}`);
      
      // TypeScript will catch errors at compile time:
      // payload.invalidField // ❌ Error: Property 'invalidField' does not exist
      // payload.userId.toUpperCase() // ✅ OK: userId is a string
      // payload.timestamp.toFixed(2) // ✅ OK: timestamp is a number
    },
    {
      maxRetries: 3,
      retryBackoffMs: 500,
      dlqEnabled: true,
    }
  );

  // Example 2: Consumer with typed payload (OrderPlaced)
  await consumeWithRetryAndDLQ(
    kafka,
    "order-processing-group",
    orderPlacedTopic,
    async (payload, ctx) => {
      // TypeScript knows the exact shape of payload including nested objects!
      console.log(`New order: ${payload.orderId}`);
      console.log(`Total: $${payload.total}`);
      
      // Iterate over items with full type safety
      for (const item of payload.items) {
        console.log(`  - ${item.productId}: ${item.quantity} x $${item.price}`);
      }
      
      // Calculate total with type safety
      const calculatedTotal = payload.items.reduce(
        (sum, item) => sum + (item.quantity * item.price),
        0
      );
      console.log(`Calculated total: $${calculatedTotal}`);
    }
  );

  console.log("🎯 Type-safe consumers are now running!");
  console.log("   - User events consumer (users.created)");
  console.log("   - Order events consumer (orders.placed)");
  
  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("\nShutting down...");
    await kafka.disconnect();
    process.exit(0);
  });
}

// Example 3: Show what happens without a schema
const rawTopic: TopicConfig = {
  name: "raw.events",
  retentionMs: 604800000, // 7 days
  cleanupPolicy: "delete",
  // No schema provided
};

async function exampleWithoutSchema() {
  // Without a schema, handler receives the raw EachMessagePayload
  await consumeWithRetryAndDLQ(
    kafka,
    "raw-group",
    rawTopic,
    async (payload, ctx) => {
      // payload is of type EachMessagePayload (from kafkajs)
      // You need to manually parse and validate
      const { message } = payload;
      const value = message.value?.toString();
      console.log("Raw message:", value);
    }
  );
}

if (require.main === module) {
  main().catch(console.error);
}

export { main, exampleWithoutSchema };
