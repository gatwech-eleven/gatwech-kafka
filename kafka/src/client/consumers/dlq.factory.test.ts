import { z } from "@gatwech/utils/zod";
import { afterAll, describe, expect, it, vi } from "vitest";
import { getTestInstance } from "../../test-utils/test-instance";
import { asConsumerGroupId, asTopicName } from "../../types/utility-types";
import { createDLQPublisher } from "./dlq.factory";

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/19
 */
describe("dlq - entry", async () => {
	// The shared setup fakes timers, and the broker needs real ones
	vi.useRealTimers();
	const {
		client,
		groupId,
		createTopic,
		joined,
		send,
		committedOffset,
		readTopic,
		close,
	} = await getTestInstance();

	afterAll(close);

	const schemaTopic = (name: string) => ({
		name,
		schema: z.object({ id: z.string() }),
		retentionMs: 604_800_000,
		cleanupPolicy: "delete" as const,
	});

	/** The only entry on the DLQ topic, once the group has moved past the message */
	const readEntry = async (group: string, topic: string, dlqTopic: string) => {
		await vi.waitFor(
			async () => expect(await committedOffset(group, topic)).toBe(1),
			{ timeout: 30_000, interval: 500 },
		);
		const entries = await readTopic(dlqTopic, { within: 3_000 });
		expect(entries).toHaveLength(1);
		return JSON.parse(entries[0] as string);
	};

	it("should carry the handler's last error, its stack and its cause", async () => {
		const topic = await createTopic();
		const dlqTopic = await createTopic(`${topic}.dlq`);
		const group = `${groupId}.handler`;
		const handler = vi
			.fn()
			.mockRejectedValueOnce(new Error("first attempt"))
			.mockRejectedValue(
				new Error("boom", { cause: new Error("database unreachable") }),
			);

		const consumer = await client.consume(group, { name: topic }, handler, {
			maxRetries: 2,
			retryBackoffMs: 10,
		});
		await joined(group);
		await send(topic, ["first"]);

		const entry = await readEntry(group, topic, dlqTopic);
		expect(entry.reason).toBe("handler");
		expect(entry.errorMessage).toBe("boom");
		expect(entry.stackTrace).toContain("Error: boom");
		expect(entry.errorCause).toBe("database unreachable");
		expect(entry.validationIssues).toBeUndefined();
		await consumer.disconnect();
	}, 60_000);

	it("should carry the validation issues when the message fails the topic's schema", async () => {
		const topic = await createTopic();
		const dlqTopic = await createTopic(`${topic}.dlq`);
		const group = `${groupId}.schema`;
		const handler = vi.fn();

		const consumer = await client.consume(group, schemaTopic(topic), handler);
		await joined(group);
		await send(topic, [JSON.stringify({ id: 123 })]);

		const entry = await readEntry(group, topic, dlqTopic);
		expect(entry.reason).toBe("schema");
		expect(entry.validationIssues).toEqual([
			{ path: "id", message: expect.any(String), code: "invalid_type" },
		]);
		expect(entry.errorMessage).toContain("id: ");
		expect(entry.stackTrace).toBeUndefined();
		expect(entry.originalValue).toEqual({ id: 123 });
		expect(handler).not.toHaveBeenCalled();
		await consumer.disconnect();
	}, 60_000);

	it("should say the message isn't JSON when it can't be parsed", async () => {
		const topic = await createTopic();
		const dlqTopic = await createTopic(`${topic}.dlq`);
		const group = `${groupId}.not-json`;
		const handler = vi.fn();

		const consumer = await client.consume(group, schemaTopic(topic), handler);
		await joined(group);
		await send(topic, ["not json"]);

		const entry = await readEntry(group, topic, dlqTopic);
		expect(entry.reason).toBe("schema");
		expect(entry.validationIssues).toEqual([
			{ path: "", message: expect.any(String), code: "invalid_json" },
		]);
		expect(entry.originalValue).toBe("not json");
		expect(handler).not.toHaveBeenCalled();
		await consumer.disconnect();
	}, 60_000);

	it("should throw an error about the publish, with the handler's error as its cause", async () => {
		const topic = await createTopic();
		const publishToDLQ = createDLQPublisher(client.getProducer());
		const handlerError = new Error("boom");

		const publish = publishToDLQ(
			{ value: "first" },
			{ reason: "handler", error: handlerError },
			{
				topic: asTopicName(topic),
				partition: 0,
				offset: "0",
				groupId: asConsumerGroupId(`${groupId}.publish`),
			},
		);

		await expect(publish).rejects.toMatchObject({
			name: "KafkaError",
			message: expect.stringContaining(`Failed to publish to ${topic}.dlq`),
			cause: handlerError,
		});
	}, 60_000);
});
