import { describe, it, expect, vi, beforeEach } from "vitest";
import { consumerExtension } from "../../src/extensions/consumer-extension";
import type { KafkaClient } from "../../src/client/core/kafka";
import type { TopicConfig } from "../../src/config/topics/topic-config-schema";
import { z } from "@gatwech/utils/zod";
import type { Consumer } from "kafkajs";

// Mock the consumeWithRetryAndDLQ function
vi.mock("../../src/client/consumers/consume-with-dlq", () => ({
  consumeWithRetryAndDLQ: vi.fn(),
}));

import { consumeWithRetryAndDLQ } from "../../src/client/consumers/consume-with-dlq";

describe("Consumer Extension", () => {
  let mockClient: KafkaClient;
  let mockConsumer: Consumer;

  beforeEach(() => {
    vi.clearAllMocks();
    
    mockClient = {} as KafkaClient;
    mockConsumer = {} as Consumer;
    
    // Setup default mock implementation
    vi.mocked(consumeWithRetryAndDLQ).mockResolvedValue(mockConsumer);
  });

  describe("Extension Setup", () => {
    it("should have correct extension name", () => {
      const extension = consumerExtension();
      expect(extension.name).toBe("consumer");
    });

    it("should setup extension and return API object", () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      expect(api).toHaveProperty("consume");
      expect(typeof api.consume).toBe("function");
    });
  });

  describe("consume() Method", () => {
    it("should delegate to consumeWithRetryAndDLQ with correct arguments", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const groupId = "test-group";
      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;
      const handler = vi.fn();
      const opts = { maxRetries: 5 };

      await api.consume(groupId, topic, handler, opts);

      expect(consumeWithRetryAndDLQ).toHaveBeenCalledWith(
        mockClient,
        groupId,
        topic,
        handler,
        opts
      );
      expect(consumeWithRetryAndDLQ).toHaveBeenCalledTimes(1);
    });

    it("should return the consumer instance", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      const result = await api.consume("group", topic, vi.fn());

      expect(result).toBe(mockConsumer);
    });

    it("should work without optional DLQ options", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      await api.consume("group", topic, vi.fn());

      expect(consumeWithRetryAndDLQ).toHaveBeenCalledWith(
        mockClient,
        "group",
        topic,
        expect.any(Function),
        undefined
      );
    });
  });

  describe("Type Inference", () => {
    it("should work with schema-based topics", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const UserSchema = z.object({ id: z.string(), name: z.string() });
      const userTopic: TopicConfig & { schema: typeof UserSchema } = {
        name: "user.events",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
        schema: UserSchema,
      } as any;

      // This should compile without errors due to type inference
      await api.consume("group", userTopic, async (payload) => {
        // TypeScript should infer payload as { id: string; name: string }
        const _id: string = payload.id;
        const _name: string = payload.name;
      });

      expect(consumeWithRetryAndDLQ).toHaveBeenCalled();
    });

    it("should work with schema-less topics", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const rawTopic: TopicConfig = {
        name: "raw.events",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      // This should compile with EachMessagePayload type
      await api.consume("group", rawTopic, async (payload) => {
        // TypeScript should infer payload as EachMessagePayload
        const _topic: string = (payload as any).topic;
        const _partition: number = (payload as any).partition;
      });

      expect(consumeWithRetryAndDLQ).toHaveBeenCalled();
    });
  });

  describe("DLQ Options Passthrough", () => {
    it("should pass through maxRetries option", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      await api.consume("group", topic, vi.fn(), { maxRetries: 10 });

      expect(consumeWithRetryAndDLQ).toHaveBeenCalledWith(
        mockClient,
        "group",
        topic,
        expect.any(Function),
        { maxRetries: 10 }
      );
    });

    it("should pass through all DLQ configuration options", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      const dlqOpts = {
        maxRetries: 5,
        retryBackoffMs: 1000,
        dlqEnabled: true,
        dlqSuffix: ".failed",
      };

      await api.consume("group", topic, vi.fn(), dlqOpts);

      expect(consumeWithRetryAndDLQ).toHaveBeenCalledWith(
        mockClient,
        "group",
        topic,
        expect.any(Function),
        dlqOpts
      );
    });
  });

  describe("Error Handling", () => {
    it("should propagate errors from consumeWithRetryAndDLQ", async () => {
      const extension = consumerExtension();
      const api = extension.setup(mockClient);

      const error = new Error("Connection failed");
      vi.mocked(consumeWithRetryAndDLQ).mockRejectedValue(error);

      const topic: TopicConfig = {
        name: "test.topic",
        retentionMs: 86400000,
        cleanupPolicy: "delete",
      } as TopicConfig;

      await expect(api.consume("group", topic, vi.fn())).rejects.toThrow(
        "Connection failed"
      );
    });
  });
});
