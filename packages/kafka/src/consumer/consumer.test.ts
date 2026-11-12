import { z } from "@gatwech/utils/zod";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import { getTestInstance } from "../test-utils/test-instance";
import { defineTopics } from "../topics";

// The shared setup fakes timers, and the broker needs real ones
vi.useRealTimers();

describe("consumer - handler", () => {
	it("should call the handler with the event the schema parsed", async () => {
		const { kafka, namespace, joined, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), age: z.coerce.number() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", age: "42" },
		});

		await vi.waitFor(() => expect(mockHandler).toHaveBeenCalledTimes(1), {
			timeout: 20_000,
			interval: 200,
		});
		expect(mockHandler.mock.calls[0]?.[0]).toMatchObject({
			event: { userId: "u1", age: 42 },
			topic: `${namespace}.user.registered`,
			partition: expect.any(Number),
			offset: "0",
			key: "u1",
		});
	}, 60_000);

	it("should tell the handler where the message came from", async () => {
		const { kafka, joined, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1" },
			correlationId: "request-42",
			headers: { "x-request-id": "r1" },
		});

		await vi.waitFor(() => expect(mockHandler).toHaveBeenCalledTimes(1), {
			timeout: 20_000,
			interval: 200,
		});
		expect(mockHandler.mock.calls[0]?.[0]).toMatchObject({
			correlationId: "request-42",
			source: kafka.options.clientId,
			headers: { "x-request-id": "r1", "event-version": "1.0" },
		});
	}, 60_000);

	it("should name the group after the client and the topic", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: async () => {},
		});

		expect(consumer.groupId).toBe(`${kafka.options.clientId}.userRegistered`);
	}, 60_000);

	it("should use the group it is given", async () => {
		const { kafka, joined, groupMembers, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const consumer = await kafka.consume({
			topic: "userRegistered",
			groupId: "payvo-test.legacy-group",
			handler: async () => {},
		});
		await joined("payvo-test.legacy-group");

		expect(consumer.groupId).toBe("payvo-test.legacy-group");
		expect(await groupMembers("payvo-test.legacy-group")).toBe(1);
	}, 60_000);

	it("should skip what was published before the group joined", async () => {
		const { kafka, joined, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		await kafka.publish({ topic: "userRegistered", event: { userId: "early" } });
		const mockHandler = vi.fn(async (_ctx: unknown) => {});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({ topic: "userRegistered", event: { userId: "late" } });

		await vi.waitFor(() => expect(mockHandler).toHaveBeenCalledTimes(1), {
			timeout: 20_000,
			interval: 200,
		});
		expect(mockHandler.mock.calls[0]?.[0]).toMatchObject({
			event: { userId: "late" },
		});
	}, 60_000);

	it("should read what was published before when it starts from the beginning", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		await kafka.publish({ topic: "userRegistered", event: { userId: "early" } });
		const mockHandler = vi.fn(async (_ctx: unknown) => {});

		await kafka.consume({
			topic: "userRegistered",
			fromBeginning: true,
			handler: mockHandler,
		});

		await vi.waitFor(() => expect(mockHandler).toHaveBeenCalledTimes(1), {
			timeout: 20_000,
			interval: 200,
		});
		expect(mockHandler.mock.calls[0]?.[0]).toMatchObject({
			event: { userId: "early" },
		});
	}, 60_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/19
 */
describe("consumer - retries and the DLQ", () => {
	it("should call the handler again when it throws, and stop when it works", async () => {
		const { kafka, namespace, joined, committedOffset, readMessages, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi
			.fn(async (_ctx: unknown) => {})
			.mockRejectedValueOnce(new Error("first"))
			.mockRejectedValueOnce(new Error("second"));
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 3, backoffMs: 10 },
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 20_000, interval: 300 },
		);
		expect(mockHandler).toHaveBeenCalledTimes(3);
		expect(
			await readMessages(`${namespace}.user.registered.dlq`, { within: 1_000 }),
		).toEqual([]);
	}, 60_000);

	it("should send the message to the DLQ with the handler's error once every attempt has failed", async () => {
		const { kafka, namespace, joined, committedOffset, readMessages, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {
			throw new Error("boom", { cause: new Error("database unreachable") });
		});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 2, backoffMs: 10 },
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 20_000, interval: 300 },
		);
		expect(mockHandler).toHaveBeenCalledTimes(2);
		const entries = await readMessages(`${namespace}.user.registered.dlq`, {
			within: 1_500,
		});
		expect(entries).toHaveLength(1);
		expect(JSON.parse(entries[0]?.value ?? "")).toMatchObject({
			reason: "handler",
			errorMessage: "boom",
			errorCause: "database unreachable",
			originalTopic: `${namespace}.user.registered`,
			consumerGroup: consumer.groupId,
			originalValue: { userId: "u1" },
		});
	}, 60_000);

	it("should send a message the schema rejects to the DLQ without calling the handler", async () => {
		const {
			kafka,
			namespace,
			joined,
			send,
			committedOffset,
			readMessages,
			close,
		} = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await send(`${namespace}.user.registered`, [JSON.stringify({ userId: 1 })]);

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 20_000, interval: 300 },
		);
		expect(mockHandler).not.toHaveBeenCalled();
		const entries = await readMessages(`${namespace}.user.registered.dlq`, {
			within: 1_500,
		});
		expect(JSON.parse(entries[0]?.value ?? "")).toMatchObject({
			reason: "schema",
			validationIssues: [{ path: "userId", code: "invalid_type" }],
			originalValue: { userId: 1 },
		});
	}, 60_000);

	it("should send a message that isn't JSON to the DLQ", async () => {
		const {
			kafka,
			namespace,
			joined,
			send,
			committedOffset,
			readMessages,
			close,
		} = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: async () => {},
		});
		await joined(consumer.groupId);

		await send(`${namespace}.user.registered`, ["not json"]);

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 20_000, interval: 300 },
		);
		const entries = await readMessages(`${namespace}.user.registered.dlq`, {
			within: 1_500,
		});
		expect(JSON.parse(entries[0]?.value ?? "")).toMatchObject({
			reason: "schema",
			validationIssues: [{ code: "invalid_json" }],
			originalValue: "not json",
		});
	}, 60_000);

	it("should publish to the DLQ it is given", async () => {
		const {
			kafka,
			namespace,
			joined,
			createTopic,
			committedOffset,
			readMessages,
			close,
		} = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const dlqTopic = await createTopic();
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 1, backoffMs: 10 },
			dlq: { topic: dlqTopic },
			handler: async () => {
				throw new Error("boom");
			},
		});
		await joined(consumer.groupId);

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 20_000, interval: 300 },
		);
		expect(await readMessages(dlqTopic, { within: 1_500 })).toHaveLength(1);
		expect(
			await readMessages(`${namespace}.user.registered.dlq`, { within: 1_000 }),
		).toEqual([]);
	}, 60_000);

	it("should keep consuming after a message went to the DLQ", async () => {
		const { kafka, namespace, joined, committedOffset, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (ctx: unknown) => {
			if ((ctx as { event: { userId: string } }).event.userId === "bad") {
				throw new Error("boom");
			}
		});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 1, backoffMs: 10 },
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publishBatch({
			topic: "userRegistered",
			events: [{ userId: "bad" }, { userId: "good" }],
		});

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(2),
			{ timeout: 20_000, interval: 300 },
		);
		expect(mockHandler).toHaveBeenCalledTimes(2);
	}, 60_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/18
 */
describe("consumer - a DLQ that can't be published to", () => {
	it("should pause the partition and keep the offset, without handling the message again", async () => {
		const { kafka, namespace, joined, committedOffset, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {
			throw new Error("boom");
		});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 2, backoffMs: 10 },
			dlq: { topic: `${namespace}.no.such.dlq`, attempts: 1 },
			handler: mockHandler,
		});
		await joined(consumer.groupId);

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		await vi.waitFor(() => expect(consumer.paused()).toHaveLength(1), {
			timeout: 60_000,
			interval: 300,
		});
		const callsWhenPaused = mockHandler.mock.calls.length;
		await new Promise((resolve) => setTimeout(resolve, 4_000));
		expect(mockHandler).toHaveBeenCalledTimes(callsWhenPaused);
		expect(callsWhenPaused).toBe(2);
		expect(
			await committedOffset(consumer.groupId, `${namespace}.user.registered`),
		).toBe(-1);
	}, 120_000);

	it("should hand the kept message to the DLQ once the partition resumes", async () => {
		const {
			kafka,
			namespace,
			joined,
			createTopic,
			committedOffset,
			readMessages,
			close,
		} = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const dlqTopic = `${namespace}.late.dlq`;
		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 1, backoffMs: 10 },
			dlq: { topic: dlqTopic, attempts: 1 },
			handler: async () => {
				throw new Error("boom");
			},
		});
		await joined(consumer.groupId);
		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });
		await vi.waitFor(() => expect(consumer.paused()).toHaveLength(1), {
			timeout: 60_000,
			interval: 300,
		});
		await createTopic(dlqTopic);

		consumer.resume();

		await vi.waitFor(
			async () =>
				expect(
					await committedOffset(consumer.groupId, `${namespace}.user.registered`),
				).toBe(1),
			{ timeout: 60_000, interval: 500 },
		);
		expect(await readMessages(dlqTopic, { within: 1_500 })).toHaveLength(1);
		expect(consumer.paused()).toEqual([]);
	}, 150_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/28
 */
describe("consumer - session", () => {
	it("should refuse retries that back off for longer than the session lasts", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const error = await kafka
			.consume({
				topic: "userRegistered",
				retry: { attempts: 10, backoffMs: 10_000 },
				session: { timeoutMs: 6_000, heartbeatIntervalMs: 1_000 },
				handler: async () => {},
			})
			.catch((reason) => reason);

		expect(error).toBeInstanceOf(KafkaError);
		expect(error.message).toBe(KAFKA_ERROR_CODES.RETRY_EXCEEDS_SESSION_TIMEOUT);
	});

	it("should accept retries that fit in the session", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const consumer = await kafka.consume({
			topic: "userRegistered",
			retry: { attempts: 3, backoffMs: 1_000 },
			session: { timeoutMs: 10_000, heartbeatIntervalMs: 1_000 },
			handler: async () => {},
		});

		expect(consumer.groupId).toBeDefined();
	}, 60_000);

	it("should refuse a heartbeat that isn't shorter than the session", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await expect(
			kafka.consume({
				topic: "userRegistered",
				session: { timeoutMs: 5_000, heartbeatIntervalMs: 5_000 },
				handler: async () => {},
			}),
		).rejects.toThrow(KAFKA_ERROR_CODES.HEARTBEAT_NOT_BELOW_SESSION_TIMEOUT);
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/22
 */
describe("consumer - lifecycle", () => {
	it("should leave its group when the client disconnects", async () => {
		const { kafka, joined, groupMembers, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: async () => {},
		});
		await joined(consumer.groupId);
		expect(await groupMembers(consumer.groupId)).toBe(1);

		await kafka.disconnect();

		await vi.waitFor(
			async () => expect(await groupMembers(consumer.groupId)).toBe(0),
			{ timeout: 20_000, interval: 300 },
		);
	}, 60_000);

	it("should leave its group when it is disconnected, and be disconnectable again", async () => {
		const { kafka, joined, groupMembers, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: async () => {},
		});
		await joined(consumer.groupId);

		await consumer.disconnect();

		await vi.waitFor(
			async () => expect(await groupMembers(consumer.groupId)).toBe(0),
			{ timeout: 20_000, interval: 300 },
		);
		await expect(kafka.disconnect()).resolves.toBeUndefined();
	}, 60_000);

	it("should stop calling the handler once disconnected", async () => {
		const { kafka, namespace, joined, send, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		const mockHandler = vi.fn(async (_ctx: unknown) => {});
		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: mockHandler,
		});
		await joined(consumer.groupId);
		await consumer.disconnect();

		await send(`${namespace}.user.registered`, [
			JSON.stringify({ userId: "u1" }),
		]);
		await new Promise((resolve) => setTimeout(resolve, 3_000));

		expect(mockHandler).not.toHaveBeenCalled();
	}, 60_000);

	it("should refuse a topic the client doesn't have", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);
		await kafka.connect();

		const error = await (kafka as any)
			.consume({ topic: "userRegistered", handler: async () => {} })
			.catch((reason: unknown) => reason);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_NOT_DECLARED);
		expect(error.cause).toBe("userRegistered");
	});

	it("should refuse to consume before it is connected", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);

		await expect(
			kafka.consume({ topic: "userRegistered", handler: async () => {} }),
		).rejects.toThrow(KAFKA_ERROR_CODES.NOT_CONNECTED);
	});

	it("should log and consume nothing when the client is not enabled", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			enabled: false,
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const consumer = await kafka.consume({
			topic: "userRegistered",
			handler: async () => {},
		});

		expect(consumer.paused()).toEqual([]);
		await expect(consumer.disconnect()).resolves.toBeUndefined();
		expect(countLogs("Kafka is disabled; not consuming")).toBe(1);
	});
});
