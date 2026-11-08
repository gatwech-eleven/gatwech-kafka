import type { IHeaders, Producer } from "kafkajs";
import { DlqMessageSchema } from "../../schemas/dlq.schema";
import type { TKafkaMessage } from "../../types";
import type { DeepReadonly } from "../../types/utility-types";
import { logger } from "../../utils/logger";

import type { TopicName, ConsumerGroupId } from "../../types/utility-types";

export type DLQContext = {
	topic: TopicName;
	partition: number;
	offset: string;
	groupId: ConsumerGroupId;
};

export type DLQMessageParts = {
	value: Buffer | string | null;
	key?: Buffer | string | null;
	headers?: IHeaders | undefined;
};

export type CreateDLQPublisherOptions = {
	dlqSuffix?: string;
};

/**
 * Create a reusable DLQ publisher consistent with createEventPublisher patterns.
 * - Validates payload against DlqMessageSchema
 * - Adds x-original-* headers for traceability
 */
export function createDLQPublisher(
	producer: Producer,
	opts: CreateDLQPublisherOptions = {},
) {
	const dlqSuffix = opts.dlqSuffix ?? ".dlq";

	return async function publishToDLQ(
		message: DLQMessageParts,
		error: Error,
		ctx: DLQContext,
	): Promise<void> {
		const dlqTopic = `${ctx.topic}${dlqSuffix}`;

		const originalValue = message.value
			? (() => {
					try {
						const str =
							typeof message.value === "string"
								? message.value
								: message.value.toString();
						return JSON.parse(str);
					} catch {
						return typeof message.value === "string"
							? message.value
							: message.value?.toString();
					}
				})()
			: undefined;

		const payload = DlqMessageSchema.parse({
			timestamp: Date.now(),
			originalTopic: ctx.topic,
			originalPartition: ctx.partition,
			originalOffset: ctx.offset,
			consumerGroup: ctx.groupId,
			errorMessage: error.message || "Message processing failed",
			stackTrace: error.stack,
			originalKey:
				typeof message.key === "string" ? message.key : message.key?.toString(),
			originalHeaders: Object.fromEntries(
				Object.entries(message.headers ?? {}).map(([k, v]) => {
					if (Array.isArray(v)) {
						const last = v[v.length - 1];
						return [k, typeof last === "string" ? last : (last?.toString() ?? "")];
					}
					return [k, typeof v === "string" ? v : (v?.toString() ?? "")];
				}),
			),
			originalValue,
		});

		const kafkaMessage: TKafkaMessage<DeepReadonly<typeof payload>> = {
			key: payload.originalKey || ctx.offset,
			value: payload,
			headers: {
				"x-original-topic": ctx.topic,
				"x-original-offset": ctx.offset,
				"x-original-partition": String(ctx.partition),
			},
		};

		try {
			await producer.send({
				topic: dlqTopic,
				messages: [
					{
						key: kafkaMessage.key,
						value: JSON.stringify(kafkaMessage.value),
						headers: kafkaMessage.headers,
					},
				],
			});
			logger.warn("Message sent to DLQ", {
				dlqTopic,
				topic: ctx.topic,
				partition: ctx.partition,
				offset: ctx.offset,
				groupId: ctx.groupId,
			});
		} catch (err) {
			logger.error("Failed to publish to DLQ", {
				dlqTopic,
				topic: ctx.topic,
				partition: ctx.partition,
				offset: ctx.offset,
				groupId: ctx.groupId,
				error: err,
			});
			throw error; // rethrow original to block offset commit
		}
	};
}
