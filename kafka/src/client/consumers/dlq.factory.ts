import type { IHeaders, Producer } from "kafkajs";
import {
	DlqMessageSchema,
	type TDlqValidationIssue,
} from "../../schemas/dlq.schema";
import type { TKafkaMessage } from "../../types";
import type { DeepReadonly } from "../../types/utility-types";
import { logger } from "../../utils/logger";
import { KafkaError } from "../core/errors";

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

/**
 * Why a message is sent to the DLQ
 */
export type DLQFailure =
	| {
			reason: "handler";
			/**
			 * What the handler threw on its last attempt
			 */
			error: unknown;
	  }
	| {
			reason: "schema";
			/**
			 * What the topic's schema rejected
			 */
			issues: TDlqValidationIssue[];
	  };

export type CreateDLQPublisherOptions = {
	dlqSuffix?: string;
};

function messageOf(error: unknown) {
	return error instanceof Error ? error.message || error.name : String(error);
}

/**
 * The fields of a DLQ entry that say why the message failed
 */
function describeFailure(failure: DLQFailure) {
	if (failure.reason === "schema") {
		const summary = failure.issues
			.map((issue) =>
				issue.path ? `${issue.path}: ${issue.message}` : issue.message,
			)
			.join("; ");
		return {
			reason: failure.reason,
			errorMessage: `The message doesn't match the topic's schema: ${summary}`,
			validationIssues: failure.issues,
		};
	}
	const { error } = failure;
	return {
		reason: failure.reason,
		errorMessage: messageOf(error),
		stackTrace: error instanceof Error ? error.stack : undefined,
		errorCause:
			error instanceof Error && error.cause !== undefined
				? messageOf(error.cause)
				: undefined,
	};
}

/**
 * Create a reusable DLQ publisher consistent with createEventPublisher patterns.
 * - Validates payload against DlqMessageSchema
 * - Adds x-original-* headers for traceability
 *
 * The entry says why the message failed: the handler's last error, or the
 * issues the topic's schema raised. A failed publish throws a `KafkaError`
 * about the publish, with the handler's error as its `cause`
 */
export function createDLQPublisher(
	producer: Producer,
	opts: CreateDLQPublisherOptions = {},
) {
	const dlqSuffix = opts.dlqSuffix ?? ".dlq";

	return async function publishToDLQ(
		message: DLQMessageParts,
		failure: DLQFailure,
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
			...describeFailure(failure),
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
			throw new KafkaError(
				`Failed to publish to ${dlqTopic}: ${messageOf(err)}`,
				failure.reason === "handler" ? failure.error : undefined,
				{ dlqTopic, publishError: err },
			);
		}
	};
}
