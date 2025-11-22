import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { z } from "zod";
import { createServicePublisherFactory } from '../../src/client/producers/service-publisher-factory';
import { generateCorrelationId } from "../../src/utils/helpers";
import { createEventPublisher } from '../../src/client/producers/event.factory';

// Spy on underlying createEventPublisher so we can assert configuration
// Spy on underlying createEventPublisher so we can assert configuration
vi.mock("../../src/client/producers/event.factory", () => ({
  createEventPublisher: vi.fn((...args: any[]) =>
    vi.fn().mockResolvedValue(undefined)
  ),
}));

// Stable time
const FIXED_DATE = new Date("2025-01-01T00:00:00.000Z");

// Test schema aligned with current contract (epoch timestamp number)
const UserSchema = z.object({
  id: z.string().uuid().or(z.string()), // allow simple string for test
  username: z.string(),
  timestamp: z.number().optional(),
  metadata: z
    .object({
      source: z.string().optional(),
      correlationId: z.string().optional(),
      customData: z.string().optional(),
    })
    .optional(),
});

type UserInput = z.input<typeof UserSchema>;

const keyDeriver = (p: UserInput) => p.id;

describe("createServicePublisherFactory", () => {
  const SERVICE_SOURCE = "test-user-service-v1";
  let factory: ReturnType<typeof createServicePublisherFactory>;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(FIXED_DATE);
    vi.clearAllMocks();
    factory = createServicePublisherFactory({} as any, SERVICE_SOURCE);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("configures underlying event publisher correctly", () => {
    const publishUser = factory("user.created", UserSchema, keyDeriver as any);
    expect(typeof publishUser).toBe("function");
    expect((createEventPublisher as any).mock.calls[0]).toEqual([
      {}, // producer
      "user.created",
      UserSchema,
      keyDeriver,
      SERVICE_SOURCE,
    ]);
  });

  it("injects timestamp, source, and new correlationId when absent", async () => {
    const publishUser = factory(
      "user.registered",
      UserSchema,
      keyDeriver as any
    );
    // Spy correlationId generator
    const cidSpy = vi.spyOn({ generateCorrelationId }, "generateCorrelationId");
    // We cannot directly override imported function due to static binding; instead rely on returned value shape

    await publishUser({ id: "u-1", username: "alice" } as any);
    const underlyingPublisher = (createEventPublisher as any).mock.results[0]
      .value; // the mocked publisher fn
    expect(underlyingPublisher).toBeDefined();
    const callArgs = (underlyingPublisher as any).mock.calls[0][0];

    expect(callArgs.id).toBe("u-1");
    expect(callArgs.username).toBe("alice");
    expect(typeof callArgs.timestamp).toBe("number");
    expect(callArgs.timestamp).toBe(FIXED_DATE.getTime());
    expect(callArgs.metadata.source).toBe(SERVICE_SOURCE);
    expect(typeof callArgs.metadata.correlationId).toBe("string");
    // correlationId should start with prefix evt_
    expect(callArgs.metadata.correlationId.startsWith("evt_")).toBe(true);
  });

  it("preserves provided correlationId and custom timestamp", async () => {
    const publishUser = factory("user.updated", UserSchema, keyDeriver as any);
    const customTs = new Date("2024-05-05T10:00:00.000Z").getTime();
    await publishUser({
      id: "u-2",
      username: "bob",
      timestamp: customTs,
      metadata: { correlationId: "existing-cid", customData: "x" },
    } as any);
    const underlyingPublisher = (createEventPublisher as any).mock.results.at(
      -1
    ).value;
    const callArgs = (underlyingPublisher as any).mock.calls[0][0];
    expect(callArgs.timestamp).toBe(customTs);
    expect(callArgs.metadata.correlationId).toBe("existing-cid");
    expect(callArgs.metadata.customData).toBe("x");
    expect(callArgs.metadata.source).toBe(SERVICE_SOURCE);
  });
});
