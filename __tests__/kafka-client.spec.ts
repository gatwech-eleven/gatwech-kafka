import { vi, describe, it, expect, beforeEach } from "vitest";

// Mock kafkajs to capture producer options and return a fake producer
vi.mock("kafkajs", () => {
  let lastProducerOpts: any = null;

  const connect = vi.fn().mockResolvedValue(undefined);
  const disconnect = vi.fn().mockResolvedValue(undefined);
  const on = vi.fn();

  const producerMock = {
    connect,
    disconnect,
    on,
  };

  class Kafka {
    constructor(_opts: any) {}
    producer(opts: any) {
      lastProducerOpts = opts;
      return producerMock;
    }
    consumer() {
      return {} as any;
    }
    admin() {
      return {
        connect: vi.fn(),
        disconnect: vi.fn(),
        describeCluster: vi.fn().mockResolvedValue({ controller: 1 }),
      } as any;
    }
  }

  return {
    Kafka,
    Partitioners: { DefaultPartitioner: "default" },
    CompressionTypes: { GZIP: "gzip" },
    logLevel: { INFO: 1 },
    __getLastProducerOpts: () => lastProducerOpts,
    __producerMock: producerMock,
  };
});

import * as kafkajs from "kafkajs";
import { KafkaClient } from "../src/client/kafka";

describe("KafkaClient.createProducer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns the underlying producer and merges overrides", () => {
    const client = new KafkaClient("test-client", ["broker:9092"]);

    const overrides = { allowAutoTopicCreation: false, customOpt: 42 } as any;
    const produced = client.createProducer(overrides as any);

    // the mocked module exposes helper accessors
    const lastOpts = (kafkajs as any).__getLastProducerOpts();

    expect(lastOpts).toBeTruthy();
    expect(lastOpts.allowAutoTopicCreation).toBe(false);
    expect(lastOpts.customOpt).toBe(42);

    // returned producer should be the mocked producer
    expect(produced).toBe((kafkajs as any).__producerMock);
  });
});
