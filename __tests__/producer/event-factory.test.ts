import { describe, it, vi, expect } from "vitest";
import { createEventPublisher } from '../../src/client/producers/event.factory';
import { ResetPasswordRequestEmailSchema } from "../../src/schemas/user-schemas";

describe("event.factory.createEventPublisher", () => {
  it("publishes validated event", async () => {
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
    });

    expect((fakeProducer.send as any).mock.calls.length).toBeGreaterThanOrEqual(
      1
    );
  });

  it("throws when payload invalid", async () => {
    const fakeProducer = { send: vi.fn().mockResolvedValue(undefined) } as any;

    const publisher = createEventPublisher(
      fakeProducer,
      "users.reset-request",
      ResetPasswordRequestEmailSchema,
      (p: any) => String((p as any).email),
      "test-source"
    );

    await expect(
      publisher({
        identifier: "email",
        token: "missing",
        timestamp: Date.now(),
      } as any)
    ).rejects.toBeDefined();
  });
});
