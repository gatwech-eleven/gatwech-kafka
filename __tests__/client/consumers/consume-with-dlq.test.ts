import { describe, expect, it, vi, beforeEach, afterEach, type Mock } from "vitest";
import { z } from "@gatwech/utils/zod";
import { consumeWithRetryAndDLQ } from '../../../src/client/consumers/consume-with-dlq';
import type { TopicConfig } from "../../../src/config/topics/topic-config-schema";
import { createDLQPublisher } from "../../../src/client/consumers/dlq.factory";
import type { KafkaClient } from "../../../src/client/core/kafka";

const RETRY_BACKOFF_MS = 1000;

// Mock DLQ factory
vi.mock("../../../src/client/consumers/dlq.factory", () => ({
  createDLQPublisher: vi.fn(),
}));

const mockPublishToDLQ = vi.fn();
vi.mocked(createDLQPublisher).mockReturnValue(mockPublishToDLQ);

const mockClient = {
  createConsumer: (config: any) => {
    const consumer = {
      on: vi.fn(),
      connect: vi.fn().mockResolvedValue(undefined),
      subscribe: vi.fn().mockResolvedValue(undefined),
      run: vi.fn().mockImplementation(async ({ eachMessage }: any) => {
        (mockClient as any).lastEachMessage = eachMessage;
      }),
    };
    return consumer;
  },
  getProducer: () => ({ send: async () => {} }),
} as unknown as KafkaClient;

// Define a topic with Zod schema
const TestSchema = z.object({ id: z.string() });
const testTopic: TopicConfig & { schema: typeof TestSchema } = {
  name: "test.topic",
  schema: TestSchema,
  // other optional fields can be omitted for this test
} as any;

describe("consumeWithRetryAndDLQ generic inference", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    vi.mocked(createDLQPublisher).mockReturnValue(mockPublishToDLQ);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("handler receives correctly typed payload", async () => {
    const handler = async (payload: { id: string }, _ctx: any) => {
      expect(typeof payload.id).toBe("string");
    };
    
    await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);
    
    await (mockClient as any).lastEachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "abc-123" })),
        offset: "0",
        key: null,
        headers: {},
      },
    });
  });

  it("should publish to DLQ when schema validation fails", async () => {
    const handler = vi.fn();
    
    await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);
    
    await (mockClient as any).lastEachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: 123 })), // Invalid type
        offset: "0",
        key: null,
        headers: {},
      },
    });

    expect(handler).not.toHaveBeenCalled();
    expect(mockPublishToDLQ).toHaveBeenCalled();
    // Check if called with an Error object
    const errorArg = mockPublishToDLQ.mock.calls[0][1];
    expect(errorArg).toBeInstanceOf(Error);
    // Zod error message might vary, just check it's an error
  });

  it("should retry then publish to DLQ when handler fails", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("Processing failed"));
    
    const consumerPromise = consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler, {
      maxRetries: 2,
      retryBackoffMs: RETRY_BACKOFF_MS,
    });
    
    await consumerPromise;

    const eachMessagePromise = (mockClient as any).lastEachMessage({
      topic: "test.topic",
      partition: 0,
      message: {
        value: Buffer.from(JSON.stringify({ id: "abc-123" })),
        offset: "0",
        key: null,
        headers: {},
      },
    });

    // Fast-forward time for retries
    await vi.runAllTimersAsync();
    await eachMessagePromise;

    expect(handler).toHaveBeenCalledTimes(2); 
    
    expect(mockPublishToDLQ).toHaveBeenCalled();
    const errorArg = mockPublishToDLQ.mock.calls[0][1];
    expect(errorArg.message).toContain("Message processing failed after retries");
  });
});
