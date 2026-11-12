import type { InternalLogger } from "@gatwech/core/env";
import type { EachMessagePayload, Producer } from "kafkajs";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import type { Topic } from "../topics";
import { type DlqFailure, publishToDlq } from "./dlq";

const MAX_BACKOFF_MS = 30_000;

const sleep = (ms: number) =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

const messageOf = (error: unknown) =>
	error instanceof Error ? error.message : String(error);

/**
 * How long to wait after the nth failure of a message: `backoffMs`, doubled
 * after each one, up to 30 seconds
 */
export const getBackoffMs = (backoffMs: number, failures: number) =>
	Math.min(backoffMs * 2 ** (failures - 1), MAX_BACKOFF_MS);

/**
 * Handles the messages of one topic for one consumer group
 * @description Each message is parsed and checked by the topic's schema, and
 * the handler is called with the event. A handler that throws is called again
 * after a back-off, `retry.attempts` times in all. A message the schema
 * rejects, or that the handler failed on every time, goes to the DLQ. When the
 * DLQ can't be published to either, the partition is paused and the error is
 * thrown, so the offset is not committed and the message is neither lost nor
 * handled again until the partition is resumed
 */
export function createMessageHandler(options: {
	/**
	 * The topic as the broker knows it
	 */
	topic: Topic;
	groupId: string;
	logger: InternalLogger;
	/**
	 * The producer the DLQ is published with. It's a function because the client
	 * builds a new one after it reconnects
	 */
	getProducer: () => Producer;
	handler: (ctx: {
		event: unknown;
		topic: string;
		partition: number;
		offset: string;
		key: string | null;
		headers: Record<string, string>;
		correlationId: string | undefined;
		source: string | undefined;
	}) => Promise<void>;
	retry: { attempts: number; backoffMs: number };
	dlq: { topic: string; attempts: number };
}) {
	const { topic, groupId, logger, retry, dlq } = options;

	return async ({ partition, message, pause }: EachMessagePayload) => {
		const location = { topic: topic.name, partition, offset: message.offset };
		const headers = Object.fromEntries(
			Object.entries(message.headers ?? {}).map(([name, value]) => [
				name,
				String(value),
			]),
		);

		let failure: DlqFailure | undefined;
		let body: unknown;
		try {
			body = JSON.parse(message.value?.toString() ?? "");
		} catch (error) {
			failure = {
				reason: "schema",
				issues: [
					{
						path: "",
						message: `The message isn't valid JSON: ${messageOf(error)}`,
						code: "invalid_json",
					},
				],
			};
		}

		let event: unknown;
		if (!failure) {
			const parsed = topic.schema.safeParse(body);
			if (parsed.success) {
				event = parsed.data;
			} else {
				failure = {
					reason: "schema",
					issues: parsed.error.issues.map((issue) => ({
						path: issue.path.map(String).join("."),
						message: issue.message,
						code: issue.code,
					})),
				};
			}
		}

		if (!failure) {
			const metadata = (body as { metadata?: Record<string, unknown> })?.metadata;
			const ctx = {
				event,
				topic: topic.name,
				partition,
				offset: message.offset,
				key: message.key?.toString() ?? null,
				headers,
				correlationId:
					headers["correlation-id"] ??
					(typeof metadata?.correlationId === "string"
						? metadata.correlationId
						: undefined),
				source:
					headers.source ??
					(typeof metadata?.source === "string" ? metadata.source : undefined),
			};

			for (let attempt = 1; attempt <= retry.attempts; attempt++) {
				try {
					await options.handler(ctx);
					return;
				} catch (error) {
					failure = { reason: "handler", error };
					logger.warn("Consumer handler failed", {
						...location,
						groupId,
						attempt,
						attempts: retry.attempts,
						error: messageOf(error),
					});
					if (attempt < retry.attempts) {
						await sleep(getBackoffMs(retry.backoffMs, attempt));
					}
				}
			}
		}

		for (let attempt = 1; attempt <= dlq.attempts; attempt++) {
			try {
				await publishToDlq({
					producer: options.getProducer(),
					dlqTopic: dlq.topic,
					message: {
						value: message.value,
						key: message.key,
						headers: message.headers,
					},
					failure: failure as DlqFailure,
					origin: {
						topic: topic.name,
						partition,
						offset: message.offset,
						groupId,
					},
				});
				logger.warn("Message sent to the DLQ", {
					...location,
					groupId,
					dlqTopic: dlq.topic,
				});
				return;
			} catch (error) {
				if (attempt < dlq.attempts) {
					logger.warn("Failed to publish to the DLQ; trying again", {
						...location,
						groupId,
						attempt,
						error: messageOf(error),
					});
					await sleep(getBackoffMs(retry.backoffMs, attempt));
					continue;
				}
				logger.error("Failed to publish to the DLQ; pausing the partition", {
					...location,
					groupId,
					attempts: dlq.attempts,
					error: messageOf(error),
				});
				pause();
				throw new KafkaError(KAFKA_ERROR_CODES.DLQ_PUBLISH_FAILED, error);
			}
		}
	};
}
