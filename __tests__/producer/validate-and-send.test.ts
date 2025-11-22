import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  validateAndSend,
  validateAndSendMany,
} from "../../src/producer/validate-and-send";
import { logger } from "../../src/utils/logger";
import type { TopicConfig } from "../../src/config/topics";
import { ResetPasswordRequestEmailSchema } from "../../src/schemas/user-schemas";
import type { Logger } from "tsdown";

const topicConfig: TopicConfig = {
  name: "topic.test",
  retentionMs: 604800000,
  cleanupPolicy: "delete",
  schema: ResetPasswordRequestEmailSchema,
};

const batchTopicConfig: TopicConfig = {
  name: "topic.batch",
  retentionMs: 604800000,
  cleanupPolicy: "delete",
  schema: ResetPasswordRequestEmailSchema,
};

describe("producer.validateAndSend helpers", () => {
  let warnSpy: any;

  beforeEach(() => {
    warnSpy = vi
      .spyOn(logger as any, "warn")
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      .mockImplementation((_msg: any, _meta?: any) => logger as any);
  });

  afterEach(() => {
    warnSpy.mockRestore();
  });

  it("validateAndSend sends a valid message via generic producer", async () => {
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;
    const fakeClient = {
      getProducer: () => fakeProducer,
      sendMessage: vi.fn(), // legacy path not used when schema present
    } as any;

    const message = {
      key: "k1",
      value: {
        identifier: "email",
        email: "ok@example.com",
        token: "tok",
        timestamp: Date.now(),
      },
      headers: {},
    };

    await validateAndSend(fakeClient, topicConfig, message);

    expect(fakeProducer.send).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessage).not.toHaveBeenCalled();
  });

  it("validateAndSend throws on invalid message", async () => {
    const fakeClient = { sendMessage: vi.fn() } as any;
    const message = {
      key: "k2",
      value: { identifier: "email", token: "no-email", timestamp: Date.now() },
    } as any;

    await expect(
      validateAndSend(fakeClient, topicConfig, message)
    ).rejects.toBeDefined();
    expect(warnSpy).toHaveBeenCalled();
  });

  it("validateAndSendMany sends a valid batch via publishBatch", async () => {
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;
    const fakeClient = {
      getProducer: () => fakeProducer,
      sendMessages: vi.fn(),
    } as any;

    const messages = [
      {
        key: "k1",
        value: {
          identifier: "email",
          email: "a@b.com",
          token: "t1",
          timestamp: Date.now(),
        },
      },
      {
        key: "k2",
        value: {
          identifier: "email",
          email: "c@d.com",
          token: "t2",
          timestamp: Date.now(),
        },
      },
    ];

    await validateAndSendMany(fakeClient, batchTopicConfig, messages as any);

    // underlying producer.send called once for batch publish
    expect(fakeProducer.send).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessages).not.toHaveBeenCalled();
  });

  it("validateAndSendMany throws when any message invalid", async () => {
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;
    const fakeClient = {
      sendMessages: vi.fn(),
      getProducer: () => fakeProducer,
    } as any;
    const messages = [
      {
        key: "k1",
        value: {
          identifier: "email",
          email: "a@b.com",
          token: "t1",
          timestamp: Date.now(),
        },
      },
      {
        key: "k2",
        value: { identifier: "email", token: "missing", timestamp: Date.now() },
      },
    ];

    await expect(
      validateAndSendMany(fakeClient, batchTopicConfig, messages as any)
    ).rejects.toBeDefined();
    expect(warnSpy).toHaveBeenCalled();
  });
});
