import { describe, it, expect, vi, afterEach } from "vitest";
import { createEventPublisher } from "../src/client/event.factory";
import { consumeWithRetryAndDLQ } from "../src/client/consume-with-dlq";
import { connectWithRetry } from "../src/client/connect-with-retry";
import { ResetPasswordRequestEmailSchema } from "../src/schemas/user-schemas";
import { logger } from "../src/utils/logger";
import * as helpers from "../src/utils/helpers";
import { vi } from "vitest";

describe("usage snapshots", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("event factory produces expected producer.send payload (snapshot)", async () => {
    // make deterministic
    vi.spyOn(helpers, "generateCorrelationId").mockReturnValue("evt-fixed-id");
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
    } as any);

    expect((fakeProducer.send as any).mock.calls.length).toBeGreaterThanOrEqual(
      1
    );
    // snapshot the first call argument (topic/messages shape)
    expect((fakeProducer.send as any).mock.calls[0][0]).toMatchSnapshot();
  });

  it("consumeWithRetryAndDLQ publishes DLQ message after retries (snapshot)", async () => {
    const sendMessage = vi.fn().mockResolvedValue(undefined);
    vi.setSystemTime(new Date("2025-01-01T00:00:00.000Z"));

    // fake client with createConsumer and sendMessage
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
      sendMessage,
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

    // ensure DLQ publish attempted
    expect(sendMessage).toHaveBeenCalled();
    // snapshot the DLQ call args (topic, message)
    expect(sendMessage.mock.calls[0]).toMatchSnapshot();

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

    await connectWithRetry(flaky, {
      maxAttempts: 5,
      initialDelayMs: 0,
      factor: 1,
    });

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
});
