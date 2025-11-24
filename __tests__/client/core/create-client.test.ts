import { describe, it, expect, vi, beforeEach } from "vitest";
import { createKafkaClient, type KafkaExtension } from '../../../src/client/core/create-client';
import { KafkaClient } from '../../../src/client/core/kafka';
import { topicProvisioningExtension } from "../../../src/extensions/topic-provisioning";
import { healthCheckExtension } from "../../../src/extensions/health-check";
import { createTopicConfig } from "../../../src/config/topics/define-topics";
import { initializeKafkaTopics } from "../../../src/config/ensure-topics";
import { z } from "@gatwech/utils/zod";

// Mock dependencies
vi.mock("../../../src/client/core/kafka");
vi.mock("../../../src/config/ensure-topics");

describe("createKafkaClient", () => {
  const mockConfig = {
    clientId: "test-client",
    brokers: ["localhost:9092"],
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should create a base KafkaClient with provided config", () => {
    const client = createKafkaClient({ config: mockConfig });
    
    expect(KafkaClient).toHaveBeenCalledWith(
      mockConfig.clientId,
      mockConfig.brokers,
      expect.objectContaining({})
    );
    expect(client).toBeInstanceOf(KafkaClient);
  });

  it("should apply extensions correctly", () => {
    const mockExtension: KafkaExtension<{ testMethod: () => string }, "testExtension"> = {
      name: "testExtension",
      setup: () => ({
        testMethod: () => "success",
      }),
    };

    const client = createKafkaClient({
      config: mockConfig,
      extensions: [mockExtension],
    });

    expect(client.extensions.testExtension.testMethod()).toBe("success");
  });

  it("should support multiple extensions", () => {
    const ext1: KafkaExtension<{ method1: () => number }, "ext1"> = {
      name: "ext1",
      setup: () => ({ method1: () => 1 }),
    };
    const ext2: KafkaExtension<{ method2: () => number }, "ext2"> = {
      name: "ext2",
      setup: () => ({ method2: () => 2 }),
    };

    const client = createKafkaClient({
      config: mockConfig,
      extensions: [ext1, ext2],
    });

    expect(client.extensions.ext1.method1()).toBe(1);
    expect(client.extensions.ext2.method2()).toBe(2);
  });

  it("should throw on extension name collision", () => {
    const ext1: KafkaExtension<{ method: () => void }> = {
      name: "duplicate",
      setup: () => ({ method: () => {} }),
    };
    const ext2: KafkaExtension<{ method: () => void }> = {
      name: "duplicate",
      setup: () => ({ method: () => {} }),
    };

    expect(() => {
      createKafkaClient({
        config: mockConfig,
        extensions: [ext1, ext2],
      });
    }).toThrow("Extension name collision: 'duplicate' is already registered.");
  });
});

describe("Extensions", () => {
  const mockClient = {
    getAdmin: vi.fn(),
    healthCheck: vi.fn(),
  } as unknown as KafkaClient;

  describe("topicProvisioningExtension", () => {
    it("should call initializeKafkaTopics with correct parameters", async () => {
      const topics = createTopicConfig({
        TEST_TOPIC: { 
          name: "test.topic",
          schema: z.object({ id: z.string() }),
        },
      });
      
      const extension = topicProvisioningExtension(topics, {
        partitions: 5,
      });
      
      const { provisionTopics } = extension.setup(mockClient);
      await provisionTopics();

      expect(mockClient.getAdmin).toHaveBeenCalled();
      expect(initializeKafkaTopics).toHaveBeenCalledWith(
        undefined, // admin mock result
        expect.objectContaining({
          topics,
          partitions: 5,
        })
      );
    });
  });

  describe("healthCheckExtension", () => {
    it("should delegate to client.healthCheck", async () => {
      const extension = healthCheckExtension();
      const { healthCheck } = extension.setup(mockClient);
      
      (mockClient.healthCheck as any).mockResolvedValue(true);
      
      const result = await healthCheck();
      
      expect(mockClient.healthCheck).toHaveBeenCalled();
      expect(result).toBe(true);
    });
  });
});
