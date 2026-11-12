import { afterAll, describe, expect, it, vi } from "vitest";
import { getTestInstance } from "../../test-utils/test-instance";

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/18
 */
describe("consume-with-dlq - failed DLQ publish", async () => {
	// The shared setup fakes timers, and the broker needs real ones
	vi.useRealTimers();
	const {
		client,
		createTopic,
		joined,
		send,
		committedOffset,
		readTopic,
		close,
	} = await getTestInstance();

	afterAll(close);

	it("should pause the partition and keep the offset when the DLQ publish fails", async () => {
		const topic = await createTopic();
		const handler = vi.fn().mockRejectedValue(new Error("boom"));

		const consumer = await client.consume(
			"group.pause",
			{ name: topic },
			handler,
			{ maxRetries: 2, retryBackoffMs: 10, dlqPublishAttempts: 2 },
		);
		await joined("group.pause");
		await send(topic, ["first", "second"]);

		await vi.waitFor(
			() => expect(consumer.paused()).toEqual([{ topic, partitions: [0] }]),
			{ timeout: 30_000, interval: 200 },
		);
		expect(handler).toHaveBeenCalledTimes(2);
		expect(await committedOffset("group.pause", topic)).toBe(-1);
		await consumer.disconnect();
	}, 60_000);

	it("should not handle the message again while the partition is paused", async () => {
		const topic = await createTopic();
		const handler = vi.fn().mockRejectedValue(new Error("boom"));

		const consumer = await client.consume(
			"group.no-redelivery",
			{ name: topic },
			handler,
			{ maxRetries: 2, retryBackoffMs: 10, dlqPublishAttempts: 2 },
		);
		await joined("group.no-redelivery");
		await send(topic, ["first", "second"]);
		await vi.waitFor(() => expect(consumer.paused()).toHaveLength(1), {
			timeout: 30_000,
			interval: 200,
		});
		const callsWhenPaused = handler.mock.calls.length;

		await new Promise((resolve) => setTimeout(resolve, 5_000));

		expect(handler).toHaveBeenCalledTimes(callsWhenPaused);
		const offsets = handler.mock.calls.map(([payload]) => payload.message.offset);
		expect(offsets).toEqual(["0", "0"]);
		await consumer.disconnect();
	}, 60_000);

	it("should deliver the kept message to the DLQ once the partition resumes", async () => {
		const topic = await createTopic();
		const handler = vi.fn(async ({ message }) => {
			if (message.value.toString() === "first") {
				throw new Error("boom");
			}
		});

		const consumer = await client.consume(
			"group.resume",
			{ name: topic },
			handler,
			{ maxRetries: 2, retryBackoffMs: 10, dlqPublishAttempts: 2 },
		);
		await joined("group.resume");
		await send(topic, ["first", "second"]);
		await vi.waitFor(() => expect(consumer.paused()).toHaveLength(1), {
			timeout: 30_000,
			interval: 200,
		});

		const dlqTopic = await createTopic(`${topic}.dlq`);
		consumer.resume([{ topic, partitions: [0] }]);

		await vi.waitFor(
			async () => expect(await committedOffset("group.resume", topic)).toBe(2),
			{ timeout: 30_000, interval: 500 },
		);
		const dlqEntries = await readTopic(dlqTopic, { within: 3_000 });
		expect(dlqEntries).toHaveLength(1);
		expect(JSON.parse(dlqEntries[0] as string).originalValue).toBe("first");
		expect(consumer.paused()).toEqual([]);
		await consumer.disconnect();
	}, 90_000);

	it("should publish to the DLQ and carry on when the DLQ topic exists", async () => {
		const topic = await createTopic();
		const dlqTopic = await createTopic(`${topic}.dlq`);
		const handler = vi.fn(async ({ message }) => {
			if (message.value.toString() === "first") {
				throw new Error("boom");
			}
		});

		const consumer = await client.consume(
			"group.carry-on",
			{ name: topic },
			handler,
			{ maxRetries: 2, retryBackoffMs: 10 },
		);
		await joined("group.carry-on");
		await send(topic, ["first", "second"]);

		await vi.waitFor(
			async () => expect(await committedOffset("group.carry-on", topic)).toBe(2),
			{ timeout: 30_000, interval: 500 },
		);
		expect(consumer.paused()).toEqual([]);
		const dlqEntries = await readTopic(dlqTopic, { within: 3_000 });
		expect(dlqEntries).toHaveLength(1);
		await consumer.disconnect();
	}, 60_000);
});
