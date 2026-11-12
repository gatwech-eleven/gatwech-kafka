import {
	describe,
	expect,
	it,
	vi,
	beforeEach,
	afterEach,
	type Mock,
} from "vitest";
import { z } from "@gatwech/utils/zod";
import { consumeWithRetryAndDLQ } from "../../../src/client/consumers/consume-with-dlq";
import type { TopicConfig } from "../../../src/config/topics/topic-config-schema";
import { createDLQPublisher } from "../../../src/client/consumers/dlq.factory";
import type { BaseKafkaClient } from "../../../src/client/core/kafka";

const RETRY_BACKOFF_MS = 1000;

// Mock DLQ factory
vi.mock("../../../src/client/consumers/dlq.factory", () => ({
	createDLQPublisher: vi.fn(),
}));

const mockPublishToDLQ = vi.fn();
vi.mocked(createDLQPublisher).mockReturnValue(mockPublishToDLQ);

const mockClient = {
	createConsumer: (config: any) => {
		const consumer = {
			on: vi.fn(),
			connect: vi.fn().mockResolvedValue(undefined),
			subscribe: vi.fn().mockResolvedValue(undefined),
			run: vi.fn().mockImplementation(async ({ eachMessage }: any) => {
				(mockClient as any).lastEachMessage = eachMessage;
			}),
			pause: vi.fn(),
		};
		(mockClient as any).lastConsumer = consumer;
		return consumer;
	},
	getProducer: () => ({ send: async () => {} }),
} as unknown as BaseKafkaClient;

// Define a topic with Zod schema
const TestSchema = z.object({ id: z.string() });
const testTopic: TopicConfig & { schema: typeof TestSchema } = {
	name: "test.topic",
	schema: TestSchema,
	// other optional fields can be omitted for this test
} as any;

describe("consumeWithRetryAndDLQ generic inference", () => {
	beforeEach(() => {
		vi.clearAllMocks();
		vi.useFakeTimers();
		vi.mocked(createDLQPublisher).mockReturnValue(mockPublishToDLQ);
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	it("handler receives correctly typed payload", async () => {
		const handler = async (payload: { id: string }, _ctx: any) => {
			expect(typeof payload.id).toBe("string");
		};

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);

		await (mockClient as any).lastEachMessage({
			topic: "test.topic",
			partition: 0,
			message: {
				value: Buffer.from(JSON.stringify({ id: "abc-123" })),
				offset: "0",
				key: null,
				headers: {},
			},
		});
	});

	it("should publish to DLQ when schema validation fails", async () => {
		const handler = vi.fn();

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);

		await (mockClient as any).lastEachMessage({
			topic: "test.topic",
			partition: 0,
			message: {
				value: Buffer.from(JSON.stringify({ id: 123 })), // Invalid type
				offset: "0",
				key: null,
				headers: {},
			},
		});

		expect(handler).not.toHaveBeenCalled();
		expect(mockPublishToDLQ).toHaveBeenCalled();
		const failure = mockPublishToDLQ.mock.calls[0]![1];
		expect(failure.reason).toBe("schema");
		expect(failure.issues[0].path).toBe("id");
	});

	it("should reject and pause the partition when the DLQ publish fails for an invalid message", async () => {
		mockPublishToDLQ.mockRejectedValueOnce(new Error("DLQ unavailable"));
		const handler = vi.fn();

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler, {
			dlqPublishAttempts: 1,
		});

		await expect(
			(mockClient as any).lastEachMessage({
				topic: "test.topic",
				partition: 0,
				message: {
					value: Buffer.from(JSON.stringify({ id: 123 })),
					offset: "0",
					key: null,
					headers: {},
				},
			}),
		).rejects.toThrow("DLQ unavailable");
		expect(handler).not.toHaveBeenCalled();
		expect((mockClient as any).lastConsumer.pause).toHaveBeenCalledWith([
			{ topic: "test.topic", partitions: [0] },
		]);
	});

	it("should reject and pause the partition when every DLQ publish fails after the handler's retries", async () => {
		mockPublishToDLQ
			.mockRejectedValueOnce(new Error("DLQ unavailable"))
			.mockRejectedValueOnce(new Error("DLQ unavailable"));
		const handler = vi.fn().mockRejectedValue(new Error("Processing failed"));

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler, {
			maxRetries: 2,
			retryBackoffMs: RETRY_BACKOFF_MS,
			dlqPublishAttempts: 2,
		});

		const eachMessage = (mockClient as any).lastEachMessage({
			topic: "test.topic",
			partition: 0,
			message: {
				value: Buffer.from(JSON.stringify({ id: "abc-123" })),
				offset: "0",
				key: null,
				headers: {},
			},
		});
		const rejection = expect(eachMessage).rejects.toThrow("DLQ unavailable");
		await vi.runAllTimersAsync();
		await rejection;

		expect(handler).toHaveBeenCalledTimes(2);
		expect(mockPublishToDLQ).toHaveBeenCalledTimes(2);
		expect((mockClient as any).lastConsumer.pause).toHaveBeenCalledTimes(1);
	});

	it("should try the DLQ publish again and not pause when a later attempt works", async () => {
		mockPublishToDLQ.mockRejectedValueOnce(new Error("DLQ unavailable"));
		const handler = vi.fn();

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler, {
			dlqPublishAttempts: 3,
			retryBackoffMs: RETRY_BACKOFF_MS,
		});

		const eachMessage = (mockClient as any).lastEachMessage({
			topic: "test.topic",
			partition: 0,
			message: {
				value: Buffer.from(JSON.stringify({ id: 123 })),
				offset: "0",
				key: null,
				headers: {},
			},
		});
		await vi.runAllTimersAsync();
		await eachMessage;

		expect(mockPublishToDLQ).toHaveBeenCalledTimes(2);
		expect((mockClient as any).lastConsumer.pause).not.toHaveBeenCalled();
	});

	it("should not reject when the message reaches the DLQ", async () => {
		const handler = vi.fn();

		await consumeWithRetryAndDLQ(mockClient, "group1", testTopic, handler);

		await expect(
			(mockClient as any).lastEachMessage({
				topic: "test.topic",
				partition: 0,
				message: {
					value: Buffer.from(JSON.stringify({ id: 123 })),
					offset: "0",
					key: null,
					headers: {},
				},
			}),
		).resolves.toBeUndefined();
		expect(mockPublishToDLQ).toHaveBeenCalledTimes(1);
	});

	it("should retry then publish to DLQ when handler fails", async () => {
		const handler = vi.fn().mockRejectedValue(new Error("Processing failed"));

		const consumerPromise = consumeWithRetryAndDLQ(
			mockClient,
			"group1",
			testTopic,
			handler,
			{
				maxRetries: 2,
				retryBackoffMs: RETRY_BACKOFF_MS,
			},
		);

		await consumerPromise;

		const eachMessagePromise = (mockClient as any).lastEachMessage({
			topic: "test.topic",
			partition: 0,
			message: {
				value: Buffer.from(JSON.stringify({ id: "abc-123" })),
				offset: "0",
				key: null,
				headers: {},
			},
		});

		// Fast-forward time for retries
		await vi.runAllTimersAsync();
		await eachMessagePromise;

		expect(handler).toHaveBeenCalledTimes(2);

		expect(mockPublishToDLQ).toHaveBeenCalled();
		const failure = mockPublishToDLQ.mock.calls[0]![1];
		expect(failure.reason).toBe("handler");
		expect(failure.error.message).toBe("Processing failed");
	});
});
