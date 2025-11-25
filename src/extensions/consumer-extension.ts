import type { Consumer } from "kafkajs";
import type { KafkaClient } from "../client/core/kafka";
import type { KafkaExtension } from "../client/core/create-client";
import type { TopicConfig } from "../config/topics/topic-config-schema";
import type { DLQOptions } from "../types";
import { 
  consumeWithRetryAndDLQ,
  type ConsumeHandlerContext,
} from "../client/consumers/consume-with-dlq";
import type { ZodType, z } from "@gatwech/utils/zod";
import { logger } from "../utils/logger";


/**
 * Consumer Extension API
 */
export interface ConsumerExtensionAPI {
  /**
   * Consume messages from a topic with automatic retry and DLQ handling.
   * Validates messages against the topic schema if provided.
   * 
   * @param groupId - Consumer group ID
   * @param topic - Topic configuration with optional schema
   * @param handler - Message handler function (payload type inferred from schema)
   * @param opts - Optional DLQ configuration
   * @returns Consumer instance
   * 
   * @example
   * // With schema (typed payload)
   * await client.extensions.consumer.consume(
   *   'my-group',
   *   userTopic, // has schema: z.object({id: z.string()})
   *   async (payload) => {
   *     console.log(payload.id); // TypeScript knows this is a string
   *   }
   * );
   * 
   * @example
   * // Without schema (raw payload)
   * await client.extensions.consumer.consume(
   *   'my-group',
   *   rawTopic,
   *   async (payload) => {
   *     console.log(payload.message.value); // EachMessagePayload
   *   }
   * );
   */
  consume<TSchema extends ZodType>(
    groupId: string,
    topic: TopicConfig & { schema: TSchema },
    handler: (payload: z.infer<TSchema>, ctx: ConsumeHandlerContext) => Promise<void>,
    opts?: DLQOptions
  ): Promise<Consumer>;
  
  consume(
    groupId: string,
    topic: TopicConfig,
    handler: (payload: import("kafkajs").EachMessagePayload, ctx: ConsumeHandlerContext) => Promise<void>,
    opts?: DLQOptions
  ): Promise<Consumer>;
}

/**
 * Consumer extension for Kafka client.
 * Provides convenient access to consumer functionality with retry and DLQ handling.
 * 
 * @example
 * ```typescript
 * const client = createKafkaClient({
 *   config: { clientId: 'my-service', brokers: ['localhost:9092'] },
 *   extensions: [consumerExtension()]
 * });
 * 
 * await client.extensions.consumer.consume(
 *   'my-group',
 *   userTopic,
 *   async (payload) => {
 *     // Handle message - payload is typed based on schema
 *   }
 * );
 * ```
 */
export const consumerExtension = (): KafkaExtension<ConsumerExtensionAPI, "consumer"> => ({
  name: "consumer",
  setup: (client: KafkaClient) => {
    logger.info("[Extension] Installing consumer extension...");

    return {
      consume: (
        groupId: string,
        topic: TopicConfig,
        handler: any,
        opts?: DLQOptions
      ): Promise<Consumer> => {
        return consumeWithRetryAndDLQ(client, groupId, topic, handler, opts);
      },
    };
  },
});
