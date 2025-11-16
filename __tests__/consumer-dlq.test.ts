import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { consumeWithRetryAndDLQ } from "../src/client/consume-with-dlq";
import { TopicConfigSchema } from "../src/config/topics/topic-config-schema";
import { ResetPasswordRequestEmailSchema } from "../src/schemas/user-schemas";
import { logger } from "../src/utils/logger";

function makePayload(value: any) {
  return {
    topic: "topic.test",
    partition: 0,
    message: {
      key: Buffer.from("k1"),
      value: Buffer.from(JSON.stringify(value)),
      headers: {},
      offset: "1",
      timestamp: Date.now().toString(),
    },
  } as any;
}

describe("consumer DLQ wrapper", () => {
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
  it("publishes to DLQ after retries exhausted", async () => {
    // use real timers because this test relies on awaited backoffs
    vi.useRealTimers();
    try {
      const fakeConsumer = {
        subscribe: vi.fn().mockResolvedValue(undefined),
        run: vi.fn().mockImplementation(async (opts: any) => {
          // Invoke eachMessage once to simulate a single message
          await opts.eachMessage(
            makePayload({
              identifier: "email",
              token: "missing",
              timestamp: Date.now(),
            })
          );
        }),
      } as any;

      const sendMessage = vi.fn().mockResolvedValue(undefined);
      const client = {
        createConsumer: vi.fn().mockResolvedValue(fakeConsumer),
        sendMessage,
      } as any;

      const topic = TopicConfigSchema.parse({
        name: "topic.test",
        schema: ResetPasswordRequestEmailSchema,
      });

      const handler = vi.fn().mockRejectedValue(new Error("fail"));

      await consumeWithRetryAndDLQ(client, "group-1", topic, handler, {
        maxRetries: 2,
        retryBackoffMs: 1,
      });
      // handler should be attempted and then DLQ publish happens
      expect(sendMessage).toHaveBeenCalledTimes(1);
      const [dlqTopic, dlqMsg] = sendMessage.mock.calls[0];
      expect(dlqTopic).toBe("topic.test.dlq");
      expect(dlqMsg.value.originalTopic).toBe("topic.test");
      expect(dlqMsg.value.originalOffset).toBe("1");
    } finally {
      vi.useFakeTimers();
    }
  });

  it("does not DLQ if succeeds before max retries", async () => {
    // use real timers because this test relies on awaited backoffs
    vi.useRealTimers();
    try {
      const fakeConsumer = {
        subscribe: vi.fn().mockResolvedValue(undefined),
        run: vi.fn().mockImplementation(async (opts: any) => {
          await opts.eachMessage(
            makePayload({
              identifier: "email",
              email: "ok@example.com",
              token: "t",
              timestamp: Date.now(),
            })
          );
        }),
      } as any;

      const sendMessage = vi.fn().mockResolvedValue(undefined);
      const client = {
        createConsumer: vi.fn().mockResolvedValue(fakeConsumer),
        sendMessage,
      } as any;

      const topic = TopicConfigSchema.parse({
        name: "topic.test",
        schema: ResetPasswordRequestEmailSchema,
      });

      let attempts = 0;
      const handler = vi.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 2) throw new Error("transient");
        return;
      });

      await consumeWithRetryAndDLQ(client, "group-1", topic, handler, {
        maxRetries: 3,
        retryBackoffMs: 1,
      });

      expect(handler).toHaveBeenCalledTimes(2);
      expect(sendMessage).not.toHaveBeenCalled();
    } finally {
      vi.useFakeTimers();
    }
  });
});
