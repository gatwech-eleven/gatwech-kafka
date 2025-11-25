import { describe, it, expect, vi, beforeEach } from "vitest";
import { createKafkaClient, type KafkaExtension } from '../../../src/client/core/create-client';
import { KafkaClient } from '../../../src/client/core/kafka';

// Mock dependencies
vi.mock("../../../src/client/core/kafka");

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

