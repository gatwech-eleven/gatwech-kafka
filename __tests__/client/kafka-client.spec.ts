import { vi, describe, it, expect, beforeEach } from "vitest";

// Mock kafkajs to capture producer options and return a fake producer
vi.mock("kafkajs", () => {
  let lastProducerOpts: any = null;
  let lastSendOpts: any = null;

  const connect = vi.fn().mockResolvedValue(undefined);
  const disconnect = vi.fn().mockResolvedValue(undefined);
  const on = vi.fn();
  const send = vi.fn().mockImplementation(async (opts: any) => {
    lastSendOpts = opts;
    return undefined;
  });

  const producerMock = {
    connect,
    disconnect,
    on,
    send,
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
    __getLastSendOpts: () => lastSendOpts,
    __producerMock: producerMock,
  };
});

import * as kafkajs from "kafkajs";
import { KafkaClient } from '../../src/client/core/kafka';

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

  it("does not auto-connect by default", async () => {
    // Arrange
    const producerMock = (kafkajs as any).__producerMock;

    // Act
    // By default, autoConnectAsync=false
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _client = new KafkaClient("test-client", ["broker:9092"]);

    // Assert
    expect(producerMock.connect).not.toHaveBeenCalled();
  });

  it("autoConnectAsync=true triggers connectAsync on construction", async () => {
    // Arrange
    const producerMock = (kafkajs as any).__producerMock;

    // Act
    // Construct with autoConnectAsync so constructor calls connectAsync()
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _client = new KafkaClient("test-client", ["broker:9092"], {
      autoConnectAsync: true,
    } as any);

    // Allow any pending microtasks to run
    await Promise.resolve();
    await Promise.resolve();

    // Assert
    expect(producerMock.connect).toHaveBeenCalled();
  });

  it("uses idempotent producer by default and sends with acks=all", async () => {
    // Arrange: construct client and validate producer config
    const client = new KafkaClient("test-client", ["broker:9092"]);
    const lastOpts = (kafkajs as any).__getLastProducerOpts();
    expect(lastOpts).toBeTruthy();
    expect(lastOpts.idempotent).toBe(true);

    // Act: connect and send a message through the helper
    await client.connect();
    // Set internal flag to allow send without full broker
    await client.sendMessage("topic.test", {
      key: "k",
      value: { ok: true },
      headers: {},
    } as any);

    // Assert: captured acks is -1 (all)
    const lastSend = (kafkajs as any).__getLastSendOpts();
    expect(lastSend).toBeTruthy();
    expect(lastSend.acks).toBe(-1);
  });
});
