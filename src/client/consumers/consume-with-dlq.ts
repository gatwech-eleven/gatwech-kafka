import type { EachMessagePayload, Consumer } from "kafkajs";
import { logger } from "../../utils/logger";
import type { DLQOptions, TKafkaMessage } from "../../types";
import {
  type DeepReadonly,
  asTopicName,
  asConsumerGroupId,
} from "../../types/utility-types";
import type { TopicConfig } from "../../config/topics/topic-config-schema";
import { createDLQPublisher } from "./dlq.factory";
import type { KafkaClient } from "../core/kafka";
import type { ZodType, z } from "@gatwech/utils/zod";
function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

/**
 * Type guard to check if a topic has a schema defined.
 */
function hasSchema(topic: TopicConfig): topic is TopicConfig & { schema: ZodType } {
  return topic.schema !== undefined && topic.schema !== null;
}

const MAX_RETRY_DELAY_MS = 30_000;

/**
 * Validates and parses message payload against topic schema.
 * @returns Parsed payload if validation succeeds, undefined if it fails.
 */
function validatePayload<TSchema extends ZodType>(
  topic: TopicConfig & { schema: TSchema },
  messageValue: Buffer | null
): z.infer<TSchema> | undefined {
  try {
    const rawData = messageValue ? JSON.parse(messageValue.toString()) : undefined;
    const result = topic.schema.safeParse(rawData);
    if (!result.success) {
      logger.warn("Schema validation failed", {
        topic: topic.name,
        errors: result.error.issues,
      });
      return undefined;
    }
    return result.data;
  } catch (e) {
    logger.warn("Failed to parse message value", {
      topic: topic.name,
      error: e,
    });
    return undefined;
  }
}

/**
 * Executes handler with exponential backoff retry logic.
 * @returns true if handler succeeded, false if all retries exhausted.
 */
async function executeWithRetry<T>(
  handler: () => Promise<void>,
  maxRetries: number,
  backoffBase: number,
  context: {
    topic: string;
    partition: number;
    offset: string;
    groupId: string;
  }
): Promise<boolean> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    logger.debug("Handler attempt", { attempt: attempt + 1, ...context });
    try {
      await handler();
      return true; // success
    } catch (err) {
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
  return false; // all retries exhausted
}

/**
 * Publishes failed message to DLQ.
 */
async function handleDLQPublish(
  publishToDLQ: ReturnType<typeof createDLQPublisher>,
  message: { value: Buffer | null; key: Buffer | null; headers?: any },
  context: {
    topic: string;
    partition: number;
    offset: string;
    groupId: string;
  }
): Promise<void> {
  try {
    logger.debug("Publishing to DLQ", context);
    await publishToDLQ(
      {
        value: message.value ?? null,
        key: message.key ?? null,
        headers: message.headers,
      },
      new Error("Message processing failed after retries"),
      {
        topic: asTopicName(context.topic),
        partition: context.partition,
        offset: context.offset,
        groupId: asConsumerGroupId(context.groupId),
      }
    );
    logger.debug("Published to DLQ", context);
  } catch (err) {
    logger.error("Failed to publish to DLQ", {
      ...context,
      error: err,
    });
  }
}

export interface ConsumeHandlerContext {
  topic: TopicConfig;
  groupId: string;
}

/**
 * Run a consumer with retry and DLQ handling. Validates against TopicConfig.schema if provided.
 */
// Overload for topics with a Zod schema – payload inferred from schema
export async function consumeWithRetryAndDLQ<TSchema extends ZodType>(
  client: KafkaClient,
  groupId: string,
  topic: TopicConfig & { schema: TSchema },
  handler: (payload: z.infer<TSchema>, ctx: ConsumeHandlerContext) => Promise<void>,
  opts?: DLQOptions
): Promise<Consumer>;
// Overload for topics without a schema – raw payload is passed
export async function consumeWithRetryAndDLQ(
  client: KafkaClient,
  groupId: string,
  topic: TopicConfig,
  handler: (payload: EachMessagePayload, ctx: ConsumeHandlerContext) => Promise<void>,
  opts?: DLQOptions
): Promise<Consumer>;
// Implementation
export async function consumeWithRetryAndDLQ(
  client: KafkaClient,
  groupId: string,
  topic: TopicConfig,
  handler: (payload: any, ctx: ConsumeHandlerContext) => Promise<void>,
  opts: DLQOptions = {}
) {
  const dlqEnabled = opts.dlqEnabled ?? true;
  const maxRetries = Math.max(1, opts.maxRetries ?? 3);
  const backoffBase = Math.max(1, opts.retryBackoffMs ?? 500);
  const dlqSuffix = opts.dlqSuffix ?? ".dlq";

  const consumer = await client.createConsumer(groupId, { groupId });
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
      let validationFailed = false;

      if (hasSchema(topic)) {
        const validated = validatePayload(topic, message.value);
        if (validated === undefined) {
          validationFailed = true;
        } else {
          parsedPayload = validated;
        }
      }

      if (validationFailed) {
        if (dlqEnabled) {
          await handleDLQPublish(publishToDLQ, message, context);
        } else {
          logger.error("Schema validation failed and DLQ disabled; giving up on message", context);
        }
        return;
      }

      const success = await executeWithRetry(
        () => handler(parsedPayload, { topic, groupId }),
        maxRetries,
        backoffBase,
        context
      );

      if (!success) {
        if (dlqEnabled) {
          await handleDLQPublish(publishToDLQ, message, context);
        } else {
          logger.error("DLQ disabled; giving up on message", context);
        }
      }
    },
  });

  return consumer;
}
