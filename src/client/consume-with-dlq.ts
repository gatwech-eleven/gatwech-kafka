import type { EachMessagePayload } from "kafkajs";
import { logger } from "../utils/logger";
import type { DLQOptions, TKafkaMessage } from "../types";
import type { DeepReadonly } from "../types/utility-types";
import type { TopicConfig } from "../config/topics/topic-config-schema";
import { createDLQPublisher } from "./dlq.factory";
import type { KafkaClient } from "./kafka";
import type { ZodType } from "@gatwech/utils/zod";
function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

export interface ConsumeHandlerContext {
  topic: TopicConfig;
  groupId: string;
}

/**
 * Run a consumer with retry and DLQ handling. Validates against TopicConfig.schema if provided.
 */
// Overload for topics with a Zod schema – payload inferred from schema
export async function consumeWithRetryAndDLQ<T>(
  client: KafkaClient,
  groupId: string,
  topic: TopicConfig & { schema: ZodType<T> },
  handler: (payload: T, ctx: ConsumeHandlerContext) => Promise<void>,
  opts: DLQOptions
): Promise<any>;
// Overload for topics without a schema – raw payload is passed
export async function consumeWithRetryAndDLQ(
  client: KafkaClient,
  groupId: string,
  topic: TopicConfig,
  handler: (payload: EachMessagePayload, ctx: ConsumeHandlerContext) => Promise<void>,
  opts: DLQOptions
): Promise<any>;
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

  const consumer = await client.createConsumer(groupId, {} as any);
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

      let attempt = 0;
      let parsedPayload: any = payload;

      if ((topic as any).schema) {
        try {
          const schema: any = (topic as any).schema;
          const result = schema.safeParse(
            message.value ? JSON.parse(message.value.toString()) : undefined
          );
          if (!result.success) {
            throw new Error("Schema validation failed");
          }
          parsedPayload = result.data;
        } catch (e) {
          // If validation fails, treat as processing failure to DLQ
          attempt = maxRetries; // jump to DLQ path below
        }
      }

      for (; attempt < maxRetries; attempt++) {
        logger.debug("consumeWithRetryAndDLQ: handler attempt", {
          attempt: attempt + 1,
        });
        try {
          await handler(parsedPayload, { topic, groupId });
          return; // success, commit offset
        } catch (err) {
          const delay = Math.min(backoffBase * 2 ** attempt, 30_000);
          logger.warn("Consumer handler failed; scheduling retry", {
            topic: topicName,
            partition,
            offset: message.offset,
            groupId,
            attempt: attempt + 1,
            delay,
            error: err,
          });
          await sleep(delay);
          logger.debug("consumeWithRetryAndDLQ: woke from sleep", {
            attempt: attempt + 1,
          });
        }
      }

      if (!dlqEnabled) {
        logger.error("DLQ disabled; giving up on message", {
          topic: topicName,
          partition,
          offset: message.offset,
          groupId,
        });
        return; // give up; offset will commit to avoid blocking partition
      }

      try {
        logger.debug("consumeWithRetryAndDLQ: publishing to DLQ", {
          topic: topicName,
          partition,
          offset: message.offset,
        });
        await publishToDLQ(
          {
            value: message.value ?? null,
            key: message.key ?? null,
            headers: message.headers,
          },
          new Error("Message processing failed after retries"),
          {
            topic: topicName,
            partition,
            offset: message.offset,
            groupId,
          }
        );
        logger.debug("consumeWithRetryAndDLQ: published to DLQ", {
          topic: topicName,
          partition,
          offset: message.offset,
        });
      } catch (err) {
        logger.error("Failed to publish to DLQ", {
          topic: topicName,
          partition,
          offset: message.offset,
          groupId,
          error: err,
        });
      }
    },
  });

  return consumer;
}
