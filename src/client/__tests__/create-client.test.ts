import { describe, it, expect, vi, beforeEach } from "vitest";
import { createKafkaClient, type KafkaExtension } from "../create-client";
import { KafkaClient } from "../kafka";
import { topicProvisioningExtension } from "../../extensions/topic-provisioning";
import { healthCheckExtension } from "../../extensions/health-check";
import { createTopicConfig } from "../../config/topics/define-topics";
import { initializeKafkaTopics } from "../../config/ensure-topics";

// Mock dependencies
vi.mock("../kafka");
vi.mock("../../config/ensure-topics");

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
    const mockExtension: KafkaExtension<{ testMethod: () => string }> = {
      name: "testExtension",
      setup: () => ({
        testMethod: () => "success",
      }),
    };

    const client = createKafkaClient({
      config: mockConfig,
      extensions: [mockExtension],
    });

    expect(client.testMethod()).toBe("success");
  });

  it("should support multiple extensions", () => {
    const ext1: KafkaExtension<{ method1: () => number }> = {
      name: "ext1",
      setup: () => ({ method1: () => 1 }),
    };
    const ext2: KafkaExtension<{ method2: () => number }> = {
      name: "ext2",
      setup: () => ({ method2: () => 2 }),
    };

    const client = createKafkaClient({
      config: mockConfig,
      extensions: [ext1, ext2],
    });

    expect(client.method1()).toBe(1);
    expect(client.method2()).toBe(2);
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
        TEST_TOPIC: { name: "test.topic" },
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
