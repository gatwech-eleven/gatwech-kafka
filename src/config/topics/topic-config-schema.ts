import { z, type ZodType } from "@gatwech/utils/zod";
import type { TopicName } from "../../types/utility-types";

/**
 * Schema for Kafka topic configuration with strict validation.
 * Supports both branded TopicName and regular strings for flexibility.
 */
export const TopicConfigSchema = z.object({
  /**
   * Topic name - can be a branded TopicName or regular string.
   * Will be validated and can be branded using asTopicName() helper.
   */
  name: z.string().min(1, "Topic name cannot be empty"),

  /**
   * Message retention time in milliseconds.
   * Default: 7 days (604,800,000 ms)
   * Note: Ignored when cleanup.policy is "compact" only.
   */
  retentionMs: z
    .number()
    .int()
    .positive("Retention must be positive")
    .default(604_800_000),

  /**
   * Cleanup policy for the topic.
   * - "delete": Time-based retention (uses retentionMs)
   * - "compact": Log compaction (retentionMs ignored)
   * - "compact,delete": Both compaction and time-based deletion
   */
  cleanupPolicy: z
    .enum(["delete", "compact", "compact,delete"])
    .default("delete"),

  /**
   * Number of partitions for this topic.
   * Optional - falls back to global default if not specified.
   */
  partitions: z
    .number()
    .int()
    .positive("Partitions must be positive")
    .optional(),

  /**
   * Replication factor for this topic.
   * Optional - falls back to global default if not specified.
   */
  replicationFactor: z
    .number()
    .int()
    .positive("Replication factor must be positive")
    .optional(),

  /**
   * Zod schema for message validation.
   * Required to ensure type safety for all topic messages.
   */
  schema: z.custom<ZodType>(),
});

/**
 * Inferred TypeScript type from TopicConfigSchema.
 * Use this for type annotations when working with topic configs.
 */
export type TopicConfig = z.infer<typeof TopicConfigSchema>;

/**
 * Input type for defining topics - allows branded TopicName.
 * This is what developers provide when defining topics.
 * Fields with defaults in the schema are optional here.
 */
export type TopicConfigInput = {
  name: string | TopicName;
  retentionMs?: number; // Has default in schema
  cleanupPolicy?: "delete" | "compact" | "compact,delete"; // Has default in schema
  partitions?: number;
  replicationFactor?: number;
  schema: ZodType; // Required for type safety
};

/**
 * Validated topic config with branded name.
 * This is what gets returned after validation.
 */
export type ValidatedTopicConfig = TopicConfig & {
  name: TopicName;
};


