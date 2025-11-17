import { describe, it, expect, vi, afterEach } from "vitest";
import { createEventPublisher } from "../src/client/event.factory";
import { consumeWithRetryAndDLQ } from "../src/client/consume-with-dlq";
import { connectWithRetry } from "../src/client/connect-with-retry";
import { ResetPasswordRequestEmailSchema } from "../src/schemas/user-schemas";
import { logger } from "../src/utils/logger";
import * as helpers from "../src/utils/helpers";
import { createServicePublisherFactory } from "../src/client/service-publisher-factory";
import { z } from "@gatwech/utils/zod";

describe("usage snapshots", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("event factory produces expected producer.send payload (snapshot)", async () => {
    // make deterministic
    vi.spyOn(helpers, "generateCorrelationId").mockReturnValue(
      "evt-fixed-id" as any
    );
    vi.setSystemTime(new Date("2025-01-01T00:00:00.000Z"));
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;

    const publisher = createEventPublisher(
      fakeProducer,
      "users.reset-request",
      ResetPasswordRequestEmailSchema,
      (p: any) => String((p as any).email),
      "test-source"
    );

    await publisher({
      identifier: "email",
      email: "ok@example.com",
      token: "tok",
      timestamp: Date.now(),
    } as any);

    expect((fakeProducer.send as any).mock.calls.length).toBeGreaterThanOrEqual(
      1
    );
    // snapshot the first call argument (topic/messages shape)
    expect((fakeProducer.send as any).mock.calls[0][0]).toMatchSnapshot();
  });

  it("consumeWithRetryAndDLQ publishes DLQ message after retries (snapshot)", async () => {
    const producerSend = vi.fn().mockResolvedValue(undefined);
    // use real timers for this test because the code awaits setTimeout-based backoffs
    vi.useRealTimers();
    vi.setSystemTime(new Date("2025-01-01T00:00:00.000Z"));

    // fake client with createConsumer and getProducer
    const client: any = {
      createConsumer: vi.fn().mockResolvedValue({
        subscribe: vi.fn().mockResolvedValue(undefined),
        run: vi.fn().mockImplementation(async ({ eachMessage }) => {
          // simulate a single message which will cause handler to throw
          await eachMessage({
            topic: "test-topic",
            partition: 0,
            message: {
              offset: "42",
              key: Buffer.from("key-1"),
              value: Buffer.from(JSON.stringify({ foo: "bar" })),
              headers: {},
            },
          } as any);
        }),
      }),
      getProducer: vi.fn().mockReturnValue({ send: producerSend }),
    };

    const topic = { name: "test-topic" } as any;

    // handler always throws
    const handler = vi.fn(async () => {
      throw new Error("handler-failed");
    });

    // run the consumer function (it returns the consumer object)
    const consumer = await consumeWithRetryAndDLQ(
      client as any,
      "grp-1",
      topic,
      handler,
      {
        maxRetries: 2,
        retryBackoffMs: 1,
        dlqEnabled: true,
        dlqSuffix: ".dlq",
      } as any
    );

    try {
      // ensure DLQ publish attempted
      expect(producerSend).toHaveBeenCalled();

      // Normalize stack paths in the DLQ payload before snapshotting
      const rawCall = producerSend.mock.calls[0][0];

      function sanitizeStackTraceInPayload(obj: any) {
        if (!obj) return obj;
        if (typeof obj === "string") {
          try {
            obj = JSON.parse(obj);
          } catch {
            return obj;
          }
        }
        if (
          obj &&
          typeof obj === "object" &&
          obj.stackTrace &&
          typeof obj.stackTrace === "string"
        ) {
          // Replace absolute path-like segments with a redaction token
          obj.stackTrace = obj.stackTrace.replace(
            /\/[^^\s:()]+(\/[^^\s:()]+)+/g,
            "<redacted-path>"
          );
          // Redact line/column numbers to keep snapshots stable when code shifts
          obj.stackTrace = obj.stackTrace.replace(
            /:\d+:?\d*/g,
            ":<redacted-pos>"
          );
        }
        return JSON.stringify(obj);
      }

      const sanitizedCall = JSON.parse(JSON.stringify(rawCall));
      if (Array.isArray(sanitizedCall.messages)) {
        sanitizedCall.messages = sanitizedCall.messages.map((m: any) => {
          const mCopy = { ...m };
          if (mCopy.value) {
            mCopy.value = sanitizeStackTraceInPayload(mCopy.value);
          }
          return mCopy;
        });
      }

      expect(sanitizedCall).toMatchSnapshot();
    } finally {
      vi.useFakeTimers();
    }

    // cleanup: if consumer exposes disconnect, call it
    if (consumer && typeof consumer.disconnect === "function") {
      await consumer.disconnect();
    }
  });

  it("connectWithRetry retries until success (snapshot logger calls)", async () => {
    const attempts: string[] = [];
    const flaky = vi.fn(async () => {
      attempts.push("attempt");
      if (attempts.length < 3) {
        throw new Error(`fail-${attempts.length}`);
      }
      return;
    });

    const warnSpy = vi
      .spyOn(logger, "warn")
      .mockImplementation(() => undefined as any);
    const infoSpy = vi
      .spyOn(logger, "info")
      .mockImplementation(() => undefined as any);

    // use real timers to allow the retry timeouts to run
    vi.useRealTimers();
    try {
      await connectWithRetry(flaky, {
        maxAttempts: 5,
        initialDelayMs: 0,
        factor: 1,
      });
    } finally {
      vi.useFakeTimers();
    }

    // capture logger calls of interest
    const warnCalls = warnSpy.mock.calls.map((c) => c.slice(0, 2));
    const infoCalls = infoSpy.mock.calls.map((c) => c.slice(0, 2));

    expect(attempts.length).toBeGreaterThanOrEqual(3);
    expect({
      attempts: attempts.length,
      warnCalls,
      infoCalls,
    }).toMatchSnapshot();
  });

  it("service publisher factory produces enriched producer.send payload (snapshot)", async () => {
    vi.setSystemTime(new Date("2025-01-01T00:00:00.000Z"));
    vi.spyOn(helpers, "generateCorrelationId").mockReturnValue(
      "svc-fixed-cid" as any
    );
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;
    const service = createServicePublisherFactory(fakeProducer, "user-svc-v1");
    // Minimal schema for this snapshot
    const MinimalSchema = z.object({
      id: z.string(),
      email: z.string().email(),
      timestamp: z.number().optional(),
      metadata: z
        .object({
          correlationId: z.string().optional(),
          source: z.string().optional(),
        })
        .optional(),
    });
    const publishUser = service(
      "users.registered",
      MinimalSchema as any,
      (p: any) => p.id
    );
    await publishUser({ id: "u-123", email: "user@example.com" });
    expect(fakeProducer.send.mock.calls[0][0]).toMatchSnapshot();
  });
});
