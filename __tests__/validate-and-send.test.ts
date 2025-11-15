import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  validateAndSend,
  validateAndSendMany,
} from "../src/producer/validate-and-send";
import { logger } from "../src/utils/logger";
import type { TopicConfig } from "../src/config/topics";
import { ResetPasswordRequestEmailSchema } from "../src/schemas/user-schemas";
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

  it("validateAndSend sends a valid message", async () => {
    const fakeClient = {
      sendMessage: vi.fn().mockResolvedValue(undefined),
    } as any;

    const message = {
      key: "k1",
      value: { identifier: "email", email: "ok@example.com", token: "tok" },
      headers: {},
    };

    await validateAndSend(fakeClient, topicConfig, message);

    expect(fakeClient.sendMessage).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessage).toHaveBeenCalledWith("topic.test", message);
  });

  it("validateAndSend throws on invalid message", async () => {
    const fakeClient = { sendMessage: vi.fn() } as any;
    const message = {
      key: "k2",
      value: { identifier: "email", token: "no-email" },
    } as any;

    await expect(
      validateAndSend(fakeClient, topicConfig, message)
    ).rejects.toBeDefined();
    expect(warnSpy).toHaveBeenCalled();
  });

  it("validateAndSendMany sends a valid batch", async () => {
    const fakeClient = {
      sendMessages: vi.fn().mockResolvedValue(undefined),
    } as any;

    const messages = [
      {
        key: "k1",
        value: { identifier: "email", email: "a@b.com", token: "t1" },
      },
      {
        key: "k2",
        value: { identifier: "email", email: "c@d.com", token: "t2" },
      },
    ];

    await validateAndSendMany(fakeClient, batchTopicConfig, messages as any);

    expect(fakeClient.sendMessages).toHaveBeenCalledTimes(1);
    expect(fakeClient.sendMessages).toHaveBeenCalledWith(
      "topic.batch",
      messages
    );
  });

  it("validateAndSendMany throws when any message invalid", async () => {
    const fakeClient = { sendMessages: vi.fn() } as any;
    const messages = [
      {
        key: "k1",
        value: { identifier: "email", email: "a@b.com", token: "t1" },
      },
      { key: "k2", value: { identifier: "email", token: "missing" } },
    ];

    await expect(
      validateAndSendMany(fakeClient, batchTopicConfig, messages as any)
    ).rejects.toBeDefined();
    expect(warnSpy).toHaveBeenCalled();
  });
});
