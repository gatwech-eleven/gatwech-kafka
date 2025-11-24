/**
 * Simple demonstration of type-safe consumer with payload inference
 */

import { z } from "@gatwech/utils/zod";
import { consumeWithRetryAndDLQ, type ConsumeHandlerContext } from '../src/client/consumers/consume-with-dlq';
import type { TopicConfig } from "../src/config/topics/topic-config-schema";
import type { KafkaClient } from "../src/client/core/kafka";

// Define a schema
const UserSchema = z.object({
  id: z.string(),
  email: z.email(),
  age: z.number(),
});

// Create a topic with schema
const userTopic = {
  name: "users.topic",
  schema: UserSchema,
  cleanupPolicy: "delete",
  retentionMs: 604_800_000,
} as TopicConfig & { schema: typeof UserSchema };

// Mock client for demonstration
const mockClient = {
  createConsumer: async () => ({
    subscribe: async () => {},
    run: async () => {},
  } as any),
  getProducer: () => ({} as any),
} as unknown as KafkaClient;

// ✅ Type inference demo - payload type is automatically inferred!
async function demo() {
  await consumeWithRetryAndDLQ(
    mockClient,
    "my-group",
    userTopic,
    async (payload, ctx) => {
      // TypeScript knows payload has:
      // - payload.id: string
      // - payload.email: string  
      // - payload.age: number
      
      console.log(`User: ${payload.email}, Age: ${payload.age}`);
      
      // This is type-safe!
      const isAdult = payload.age >= 18;
      
      // TypeScript will error on invalid properties:
      // payload.invalid // ❌ Error: Property 'invalid' does not exist
    }
  );
}

export { demo };
