import { z } from "@gatwech/utils/zod";
import type { DeepReadonly, TopicName } from "../../types/utility-types";
import { asTopicName } from "../../types/utility-types";
import {
  TopicConfigSchema,
  type TopicConfig,
  type TopicConfigInput,
  type ValidatedTopicConfig,
} from "./topic-config-schema";

/**
 * Create a collection of Kafka topic configurations with strict type safety and validation.
 *
 * This function validates all topic configurations using Zod and returns
 * a readonly object with branded TopicName types for compile-time safety.
 *
 * @example
 * ```typescript
 * import { createTopicConfig } from '@gatwech/kafka';
 * import { UserRegisteredSchema, NotificationSentSchema } from './schemas';
 *
 * export const notificationTopics = createTopicConfig({
 *   USER_REGISTERED: {
 *     name: 'user.registered',
 *     schema: UserRegisteredSchema,
 *     retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
 *     cleanupPolicy: 'delete',
 *     partitions: 3,
 *   },
 *   NOTIFICATION_SENT: {
 *     name: 'notification.sent',
 *     schema: NotificationSentSchema,
 *     retentionMs: 30 * 24 * 60 * 60 * 1000, // 30 days
 *     cleanupPolicy: 'compact,delete',
 *   },
 * });
 *
 * // Type-safe access
 * const topicName = notificationTopics.USER_REGISTERED.name; // TopicName (branded)
 * ```
 *
 * @template T - Record of topic configurations
 * @param topics - Object mapping topic keys to their configurations
 * @returns Deeply readonly, validated topic configurations with branded names
 * @throws {z.ZodError} If any topic configuration is invalid
 */
export function createTopicConfig<T extends Record<string, TopicConfigInput>>(
  topics: T
): { readonly [K in keyof T]: ValidatedTopicConfig } {
  const validated: Record<string, ValidatedTopicConfig> = {};

  for (const [key, config] of Object.entries(topics)) {
    // Validate using Zod schema
    const parsed = TopicConfigSchema.parse(config);

    // Brand the topic name for type safety
    validated[key] = {
      ...parsed,
      name: asTopicName(parsed.name),
    } as ValidatedTopicConfig;
  }

  // Return as readonly but not deeply readonly to maintain type compatibility
  return Object.freeze(validated) as {
    readonly [K in keyof T]: ValidatedTopicConfig;
  };
}

/**
 * Create a single Kafka topic configuration with validation.
 *
 * @example
 * ```typescript
 * const userTopic = createSingleTopicConfig({
 *   name: 'user.registered',
 *   schema: UserRegisteredSchema,
 *   retentionMs: 7 * 24 * 60 * 60 * 1000,
 *   cleanupPolicy: 'delete',
 * });
 * ```
 *
 * @param config - Topic configuration
 * @returns Validated topic configuration with branded name
 * @throws {z.ZodError} If configuration is invalid
 */
export function createSingleTopicConfig(
  config: TopicConfigInput
): DeepReadonly<ValidatedTopicConfig> {
  const parsed = TopicConfigSchema.parse(config);
  return {
    ...parsed,
    name: asTopicName(parsed.name),
  } as DeepReadonly<ValidatedTopicConfig>;
}

/**
 * Convert a topics object to an array format for easier iteration.
 *
 * @example
 * ```typescript
 * const topicsArray = topicsToArray(notificationTopics);
 * for (const topic of topicsArray) {
 *   console.log(topic.name, topic.retentionMs);
 * }
 * ```
 */
export function topicsToArray<T extends Record<string, ValidatedTopicConfig>>(
  topics: T
): ReadonlyArray<ValidatedTopicConfig> {
  return Object.values(topics);
}

