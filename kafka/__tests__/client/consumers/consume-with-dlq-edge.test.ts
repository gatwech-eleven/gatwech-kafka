import { describe, it, expect, vi } from "vitest";
import { consumeWithRetryAndDLQ } from "../../../src/client/consumers/consume-with-dlq";

describe("consumeWithRetryAndDLQ - DLQ publishing edge case", () => {
	it("publishes to DLQ after retries when handler keeps failing", async () => {
		const fakeSend = vi.fn().mockResolvedValue(undefined);
		const fakeProducer = { send: fakeSend } as any;

		// Consumer that will call provided eachMessage once
		const fakeConsumer = {
			on: vi.fn(),
			subscribe: vi.fn().mockResolvedValue(undefined),
			connect: vi.fn().mockResolvedValue(undefined),
			disconnect: vi.fn().mockResolvedValue(undefined),
			run: vi.fn().mockImplementation(async ({ eachMessage }: any) => {
				// simulate a single incoming message
				const payload = {
					topic: "topic.test",
					partition: 0,
					message: {
						key: Buffer.from("k1"),
						value: Buffer.from(JSON.stringify({ email: "dlq@example.com" })),
						offset: "42",
						headers: {},
					},
				};
				// debug
				console.log("fakeConsumer.run: invoking eachMessage");
				await eachMessage(payload);
				console.log("fakeConsumer.run: eachMessage finished");
				return Promise.resolve();
			}),
		} as any;

		const fakeClient: any = {
			createConsumer: vi.fn().mockReturnValue(fakeConsumer),
			getProducer: vi.fn().mockReturnValue(fakeProducer),
		};

		const topicConfig: any = {
			name: "topic.test",
		};

		// handler that always throws to trigger DLQ path
		const handler = vi.fn().mockImplementation(async () => {
			console.log("handler invoked: throwing");
			throw new Error("boom");
		});

		// Use real timers for this test so sleep resolves
		vi.useRealTimers();
		await consumeWithRetryAndDLQ(
			fakeClient as any,
			"grp-1",
			topicConfig,
			handler,
			{
				dlqEnabled: true,
				maxRetries: 2,
				retryBackoffMs: 1,
				dlqSuffix: ".dlq",
			},
		);
		vi.useFakeTimers();

		// Assert producer.send called once to DLQ
		expect(fakeSend).toHaveBeenCalled();
		const callArg = fakeSend.mock.calls[0][0];
		expect(callArg.topic).toBe("topic.test.dlq");
		// Inspect sent message payload JSON
		const sentMessage = callArg.messages[0];
		const parsed = JSON.parse(sentMessage.value as string);
		expect(parsed).toHaveProperty("originalTopic", "topic.test");
		expect(parsed).toHaveProperty("originalOffset", "42");
		expect(parsed).toHaveProperty("errorMessage");
		expect(parsed.originalValue).toEqual({ email: "dlq@example.com" });
	});
});
