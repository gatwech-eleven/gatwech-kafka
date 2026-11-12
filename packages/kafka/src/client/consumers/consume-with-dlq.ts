import type { EachMessagePayload, Consumer, IHeaders } from "kafkajs";
import { logger } from "../../utils/logger";
import type { DLQOptions } from "../../types";
import { asTopicName, asConsumerGroupId } from "../../types/utility-types";
import { createDLQPublisher, type DLQFailure } from "./dlq.factory";
import type { BaseKafkaClient } from "../core/kafka";
import type { ZodType, z } from "@gatwech/utils/zod";
import type { TopicConfig } from "../../config/topics/topic-config-schema";
import type { TDlqValidationIssue } from "../../schemas/dlq.schema";

function sleep(ms: number) {
	return new Promise((res) => setTimeout(res, ms));
}

/**
 * Type guard to check if a topic has a schema defined.
 */
function hasSchema(
	topic: AnyTopic,
): topic is TopicConfig & { schema: ZodType } {
	return !!topic.schema;
}

const MAX_RETRY_DELAY_MS = 30_000;

/**
 * Parses a message's value against the topic's schema
 *
 * @returns the parsed payload, or the issues that make the message invalid
 */
function validatePayload<TSchema extends ZodType>(
	topic: TopicConfig & { schema: TSchema },
	messageValue: Buffer | null,
):
	| { valid: true; data: z.infer<TSchema> }
	| { valid: false; issues: TDlqValidationIssue[] } {
	let rawData: unknown;
	try {
		rawData = messageValue ? JSON.parse(messageValue.toString()) : undefined;
	} catch (e) {
		logger.warn("Failed to parse message value", {
			topic: topic.name,
			error: e,
		});
		const detail = e instanceof Error ? e.message : String(e);
		return {
			valid: false,
			issues: [
				{
					path: "",
					message: `The message isn't valid JSON: ${detail}`,
					code: "invalid_json",
				},
			],
		};
	}
	const result = topic.schema.safeParse(rawData);
	if (!result.success) {
		logger.warn("Schema validation failed", {
			topic: topic.name,
			errors: result.error.issues,
		});
		const issues = result.error.issues.map((issue) => ({
			path: issue.path.map(String).join("."),
			message: issue.message,
			code: issue.code,
		}));
		return { valid: false, issues };
	}
	return { valid: true, data: result.data };
}

/**
 * Executes handler with exponential backoff retry logic.
 *
 * @returns whether the handler succeeded and, when every attempt failed, what
 * the last attempt threw
 */
async function executeWithRetry(
	handler: () => Promise<void>,
	maxRetries: number,
	backoffBase: number,
	context: {
		topic: string;
		partition: number;
		offset: string;
		groupId: string;
	},
): Promise<{ succeeded: true } | { succeeded: false; error: unknown }> {
	let lastError: unknown;
	for (let attempt = 0; attempt < maxRetries; attempt++) {
		logger.debug("Handler attempt", { attempt: attempt + 1, ...context });
		try {
			await handler();
			return { succeeded: true };
		} catch (err) {
			lastError = err;
			const delay = Math.min(backoffBase * 2 ** attempt, MAX_RETRY_DELAY_MS);
			logger.warn("Consumer handler failed; scheduling retry", {
				...context,
				attempt: attempt + 1,
				delay,
				error: err,
			});
			if (attempt < maxRetries - 1) {
				await sleep(delay);
				logger.debug("Woke from sleep", { attempt: attempt + 1 });
			}
		}
	}
	return { succeeded: false, error: lastError };
}

/**
 * Publishes a failed message to the DLQ, trying `attempts` times
 *
 * When every attempt fails it pauses the partition and throws, so the offset
 * isn't committed and the message is neither lost nor handled again until the
 * partition is resumed
 */
async function handleDLQPublish(
	publishToDLQ: ReturnType<typeof createDLQPublisher>,
	message: { value: Buffer | null; key: Buffer | null; headers?: IHeaders },
	failure: DLQFailure,
	context: {
		topic: string;
		partition: number;
		offset: string;
		groupId: string;
	},
	options: {
		attempts: number;
		backoffBase: number;
		pausePartition: () => void;
	},
): Promise<void> {
	for (let attempt = 1; ; attempt++) {
		try {
			logger.debug("Publishing to DLQ", { ...context, attempt });
			await publishToDLQ(
				{
					value: message.value ?? null,
					key: message.key ?? null,
					headers: message.headers,
				},
				failure,
				{
					topic: asTopicName(context.topic),
					partition: context.partition,
					offset: context.offset,
					groupId: asConsumerGroupId(context.groupId),
				},
			);
			logger.debug("Published to DLQ", context);
			return;
		} catch (err) {
			if (attempt < options.attempts) {
				const delay = Math.min(
					options.backoffBase * 2 ** (attempt - 1),
					MAX_RETRY_DELAY_MS,
				);
				logger.warn("Failed to publish to DLQ; trying again", {
					...context,
					attempt,
					delay,
					error: err,
				});
				await sleep(delay);
				continue;
			}
			logger.error("Failed to publish to DLQ; pausing the partition", {
				...context,
				attempts: options.attempts,
				error: err,
			});
			options.pausePartition();
			throw err;
		}
	}
}

import type {
	AnyTopic,
	RawTopic,
	SchemaConsumerPayload,
	RawConsumerPayload,
	ConsumerDlqOptions,
} from "../../types/consumer";

// ... existing imports ...

export interface ConsumeHandlerContext {
	topic: AnyTopic;
	groupId: string;
}

/**
 * Run a consumer with retry and DLQ handling. Validates against TopicConfig.schema if provided.
 */
// Overload for topics with a Zod schema – payload inferred from schema
export async function consumeWithRetryAndDLQ<TSchema extends ZodType>(
	client: BaseKafkaClient,
	groupId: string,
	topic: TopicConfig & { schema: TSchema },
	handler: (
		payload: SchemaConsumerPayload<TSchema>,
		ctx: ConsumeHandlerContext,
	) => Promise<void>,
	opts?: ConsumerDlqOptions,
): Promise<Consumer>;

// Overload for topics without a schema – raw payload is passed
export async function consumeWithRetryAndDLQ<TRawTopic extends RawTopic>(
	client: BaseKafkaClient,
	groupId: string,
	topic: TRawTopic,
	handler: (
		payload: RawConsumerPayload,
		ctx: ConsumeHandlerContext,
	) => Promise<void>,
	opts?: ConsumerDlqOptions,
): Promise<Consumer>;

// Implementation
export async function consumeWithRetryAndDLQ(
	client: BaseKafkaClient,
	groupId: string,
	topic: AnyTopic,
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	handler: (payload: any, ctx: ConsumeHandlerContext) => Promise<void>,
	/**
	 * Using 'any' for implementation to support both overload signatures:
	 * - Typed payload (z.infer<TSchema>) when schema is present
	 * - Raw EachMessagePayload when schema is absent
	 */
	opts: ConsumerDlqOptions = {},
) {
	const dlqEnabled = opts.dlqEnabled ?? true;
	const maxRetries = Math.max(1, opts.maxRetries ?? 3);
	const backoffBase = Math.max(1, opts.retryBackoffMs ?? 500);
	const dlqSuffix = opts.dlqSuffix ?? ".dlq";
	const dlqPublishAttempts = Math.max(1, opts.dlqPublishAttempts ?? 3);

	// Create consumer with proper configuration
	const consumer = client.createConsumer({
		groupId,
		retry: {
			maxRetryTime: 30000,
			initialRetryTime: 1000,
			factor: 2,
			multiplier: 2,
			retries: 5,
		},
	});

	consumer.on("consumer.connect", () => {
		logger.info("Kafka consumer connected", { groupId });
	});

	consumer.on("consumer.disconnect", () => {
		logger.info("Kafka consumer disconnected", { groupId });
	});

	consumer.on("consumer.network.request_timeout", (payload: unknown) => {
		logger.warn("Kafka consumer network request timeout", { groupId, payload });
	});

	consumer.on("consumer.crash", (err: unknown) => {
		logger.error("Kafka consumer crash", { groupId, error: err });
	});

	await consumer.connect();
	await consumer.subscribe({ topic: topic.name, fromBeginning: false });

	const publishToDLQ = createDLQPublisher(client.getProducer(), { dlqSuffix });

	await consumer.run({
		eachMessage: async (payload) => {
			const { topic: topicName, partition, message } = payload;
			logger.debug("consumeWithRetryAndDLQ: eachMessage invoked", {
				topic: topicName,
				partition,
				offset: message.offset,
			});

			const context = {
				topic: topicName,
				partition,
				offset: message.offset,
				groupId,
			};

			let parsedPayload: EachMessagePayload | unknown = payload;
			let failure: DLQFailure | undefined;

			if (hasSchema(topic)) {
				const validation = validatePayload(topic, message.value);
				if (validation.valid) {
					parsedPayload = validation.data;
				} else {
					failure = { reason: "schema", issues: validation.issues };
				}
			}

			if (!failure) {
				const outcome = await executeWithRetry(
					() => handler(parsedPayload, { topic, groupId }),
					maxRetries,
					backoffBase,
					context,
				);
				if (outcome.succeeded) {
					return;
				}
				failure = { reason: "handler", error: outcome.error };
			}

			if (!dlqEnabled) {
				logger.error(
					failure.reason === "schema"
						? "Schema validation failed and DLQ disabled; giving up on message"
						: "DLQ disabled; giving up on message",
					context,
				);
				return;
			}

			await handleDLQPublish(publishToDLQ, message, failure, context, {
				attempts: dlqPublishAttempts,
				backoffBase,
				pausePartition: () =>
					consumer.pause([{ topic: topicName, partitions: [partition] }]),
			});
		},
	});

	return consumer;
}
