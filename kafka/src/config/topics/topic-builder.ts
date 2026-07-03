import type { z } from "@gatwech/utils/zod";
import type { TopicName } from "../../types/utility-types";
import { asTopicName } from "../../types/utility-types";
import {
  TopicConfigSchema,
  type TopicConfigInput,
  type ValidatedTopicConfig,
} from "./topic-config-schema";

/**
 * Fluent builder for creating Kafka topic configurations.
 *
 * Provides a chainable API for building topic configs with compile-time
 * type safety and runtime validation.
 *
 * @example
 * ```typescript
 * const topic = new TopicConfigBuilder('user.registered')
 *   .withSchema(UserRegisteredSchema)
 *   .withRetention(7 * 24 * 60 * 60 * 1000) // 7 days
 *   .withCleanupPolicy('delete')
 *   .withPartitions(3)
 *   .withReplicationFactor(2)
 *   .build();
 * ```
 */
export class TopicConfigBuilder {
  private config: Partial<TopicConfigInput>;

  /**
   * Create a new topic configuration builder.
   *
   * @param name - Topic name (will be branded as TopicName)
   */
  constructor(name: string | TopicName) {
    this.config = { name: String(name) };
  }

  /**
   * Set the Zod schema for message validation.
   *
   * @param schema - Zod schema that validates message payloads
   * @returns This builder for chaining
   */
  withSchema<T extends z.ZodType>(schema: T): this {
    this.config.schema = schema;
    return this;
  }

  /**
   * Set the retention time in milliseconds.
   *
   * @param retentionMs - Retention time in milliseconds
   * @returns This builder for chaining
   */
  withRetention(retentionMs: number): this {
    this.config.retentionMs = retentionMs;
    return this;
  }

  /**
   * Set the retention time using a more readable format.
   *
   * @example
   * ```typescript
   * builder.withRetentionDays(7)  // 7 days
   * builder.withRetentionHours(24) // 24 hours
   * ```
   */
  withRetentionDays(days: number): this {
    this.config.retentionMs = days * 24 * 60 * 60 * 1000;
    return this;
  }

  /**
   * Set the retention time in hours.
   */
  withRetentionHours(hours: number): this {
    this.config.retentionMs = hours * 60 * 60 * 1000;
    return this;
  }

  /**
   * Set the cleanup policy.
   *
   * @param policy - Cleanup policy: "delete", "compact", or "compact,delete"
   * @returns This builder for chaining
   */
  withCleanupPolicy(
    policy: "delete" | "compact" | "compact,delete"
  ): this {
    this.config.cleanupPolicy = policy;
    return this;
  }

  /**
   * Set the number of partitions.
   *
   * @param partitions - Number of partitions (must be positive)
   * @returns This builder for chaining
   */
  withPartitions(partitions: number): this {
    this.config.partitions = partitions;
    return this;
  }

  /**
   * Set the replication factor.
   *
   * @param replicationFactor - Replication factor (must be positive)
   * @returns This builder for chaining
   */
  withReplicationFactor(replicationFactor: number): this {
    this.config.replicationFactor = replicationFactor;
    return this;
  }

  /**
   * Build and validate the topic configuration.
   *
   * @returns Validated topic configuration with branded name
   * @throws {z.ZodError} If the configuration is invalid
   */
  build(): ValidatedTopicConfig {
    const parsed = TopicConfigSchema.parse(this.config);
    return {
      ...parsed,
      name: asTopicName(parsed.name),
    } as ValidatedTopicConfig;
  }

  /**
   * Build without validation (unsafe - use only when you're certain config is valid).
   *
   * @returns Topic configuration without validation
   */
  buildUnsafe(): ValidatedTopicConfig {
    return {
      ...this.config,
      name: asTopicName(this.config.name!),
    } as ValidatedTopicConfig;
  }
}

/**
 * Create a new topic configuration builder.
 * Convenience function for creating builders without `new` keyword.
 *
 * @example
 * ```typescript
 * const topic = topicBuilder('user.registered')
 *   .withSchema(UserRegisteredSchema)
 *   .withRetentionDays(7)
 *   .build();
 * ```
 */
export function topicBuilder(name: string | TopicName): TopicConfigBuilder {
  return new TopicConfigBuilder(name);
}
