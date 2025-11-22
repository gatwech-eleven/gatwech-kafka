// Test for generic payload inference in consumeWithRetryAndDLQ
import { z } from "@gatwech/utils/zod";
import { consumeWithRetryAndDLQ } from "../consume-with-dlq";
import type { TopicConfig } from "../../config/topics/topic-config-schema";

// Minimal mock KafkaClient
const mockClient = {
  createConsumer: async (groupId: string, _opts: any) => {
    return {
      subscribe: async (_: any) => {},
      run: async ({ eachMessage }: any) => {
        // Simulate a single message
        await eachMessage({
          topic: "test.topic",
          partition: 0,
          message: {
            value: Buffer.from(JSON.stringify({ id: "abc-123" })),
            offset: "0",
            key: null,
            headers: {},
          },
        });
      },
    };
  },
  getProducer: () => ({ send: async () => {} }),
} as any;

// Define a topic with Zod schema
const testTopic: TopicConfig & { schema: typeof z.object } = {
  name: "test.topic",
  schema: z.object({ id: z.string() }),
  // other optional fields can be omitted for this test
} as any;

describe("consumeWithRetryAndDLQ generic inference", () => {
  it("handler receives correctly typed payload", async () => {
    const handler = async (payload: { id: string }, _ctx: any) => {
      // TypeScript should infer payload.id as string
      expect(typeof payload.id).toBe("string");
    };
    await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);
  });
});
