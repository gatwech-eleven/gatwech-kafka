import "./utility-types-CU44GOxi.mjs";
import { a as ISerializer, c as KafkaMetrics, d as ReconnectionOptions, f as ResilienceOptions, g as WithMetadata, h as TopicProvisioningOptions, i as DLQOptions, l as Logger, m as TKafkaMessage, n as Consumer, o as JsonSerializer, p as RetryOptions, r as ConsumerConfig, s as KafkaClientOptions, t as CircuitBreakerOptions } from "./index-NNR_qWT3.mjs";
import { C as ModularKafkaClient, E as BaseProducer, S as KafkaExtension, T as KafkaClient, _ as ConnectRetryOptions, a as ConsumeHandlerContext, b as GatwechKafkaExtensions, c as createServicePublisherFactory, d as createPublisher, f as GenericKafkaProducer, g as DEFAULT_OPTIONS, h as ReadyToPublishMessage, i as createDLQPublisher, l as CreatePublisherOptions, m as PartitionKeyFn, n as DLQContext, o as consumeWithRetryAndDLQ, p as KeyDeriver, r as DLQMessageParts, s as EventPublisher, t as CreateDLQPublisherOptions, u as createEventPublisher, v as connectWithRetry, w as createKafkaClient, x as KafkaClientConfig, y as CreateKafkaClientConfig } from "./index-CGKbRkt5.mjs";
import { i as ValidatedTopicConfig, n as TopicConfigInput, r as TopicConfigSchema, t as TopicConfig } from "./topic-config-schema-DPJTY7x_.mjs";
import { _ as waitForLeaders, a as createSingleTopicConfig, c as createMissingTopics, d as TopicDef, f as TopicMap, g as validateTopics, h as toConfigEntries, i as topicBuilder, l as CONFIG_RESOURCE_TYPE_TOPIC, m as safeDescribeConfigs, n as updateTopicConfigs, o as createTopicConfig, p as asTopicDefs, r as TopicConfigBuilder, s as topicsToArray, t as updatePartitionCounts, u as TopicConfigShape, v as withRetry } from "./index-CgHlSNmS.mjs";
import * as kafkajs0 from "kafkajs";
import { Admin, Consumer as Consumer$1, Producer } from "kafkajs";
import { ZodType, z } from "@gatwech/utils/zod";

//#region src/config/initialize.topics.d.ts
/**
 * Options for topic provisioning defaults.
 */
type EnsureOpts = {
  /**
   * Application-defined topics (from `createTopicConfig()`).
   * This is the recommended way to provide topics.
   */
  topics?: Record<string, ValidatedTopicConfig | TopicConfig>;
  /** Default number of partitions for new topics (overridden by per-topic config if provided). */
  partitions?: number;
  /** Default replication factor for new topics (overridden by per-topic config if provided). */
  replicationFactor?: number;
  /** Max time to wait for leaders after create/partition increase, in milliseconds. */
  waitForLeadersMs?: number;
};
/**
 * Initialize Kafka topics with application-level configurations.
 *
 * **What this does:**
 * 1. **Create** any missing topics with initial `numPartitions`, `replicationFactor`, and `configEntries`.
 * 2. **Increase** partitions when current count is lower than desired (Kafka only allows increasing).
 * 3. **Apply/patch** topic configs (e.g., `retention.ms`, `cleanup.policy`) on both existing and newly created topics.
 *
 * **Notes & caveats:**
 * - To combine compaction **and** time-based deletion, set `cleanup.policy` to `"compact,delete"`.
 * - `alterConfigs` and `describeConfigs` may require admin ACLs; failures are logged as warnings and do not stop startup.
 * - By default this function manages the Admin connection; pass `manageConnection=false` if the caller handles connect/disconnect.
 *
 * @example Application-level topics
 * ```ts
 * import { kafkaClient } from '@/config/kafka';
 * import { initializeKafkaTopics, createTopicConfig } from '@gatwech/kafka';
 *
 * const myTopics = createTopicConfig({
 *   USER_REGISTERED: {
 *     name: 'user.registered',
 *     schema: UserRegisteredSchema,
 *     retentionMs: 7 * 24 * 60 * 60 * 1000,
 *   },
 * });
 *
 * const admin = kafkaClient.getAdmin();
 * await initializeKafkaTopics(admin, {
 *   topics: myTopics,
 *   partitions: 3,
 *   replicationFactor: 1,
 * });
 * ```
 *
 * @param admin - KafkaJS Admin client.
 * @param opts - Initialization options.
 * @param opts.topics - Application-defined topics (from `createTopicConfig()`).
 * @param opts.partitions - Default partitions for new topics (default: 3).
 * @param opts.replicationFactor - Default replication factor for new topics (default: 1).
 * @param opts.waitForLeadersMs - Max time to wait for leader election after changes (default: 15000ms).
 * @param manageConnection - If `true`, this function connects/disconnects the Admin (default: `true`).
 * @returns Promise that resolves when topics are initialized.
 * @throws Propagates unexpected admin errors (topic creation races are handled gracefully).
 */
declare function initializeKafkaTopics(admin: Admin, opts?: EnsureOpts, manageConnection?: boolean): Promise<void>;
//#endregion
//#region src/extensions/partition-analysis.d.ts
/** Defines the throughput metrics needed for the 4-Pillar calculation. */
interface ThroughputMetrics {
  /** Target total throughput (T) in MB/s. */
  targetThroughputMBps: number;
  /** Producer throughput per partition (P) in MB/s. */
  producerPerPartitionMBps: number;
  /** Consumer throughput per partition (C) in MB/s (slower part). */
  consumerPerPartitionMBps: number;
  /** Desired consumer parallelism (max active instances). */
  desiredParallelism: number;
  /** Projected growth factor (e.g., 2.0 for 100% growth). */
  growthFactor: number;
}
interface PartitionSkewResult {
  status: 'BALANCED' | 'SKEWED';
  hotPartitions: string[];
}
/**
 * Calculates the optimal number of partitions based on the 4-Pillar Framework
 * derived from the video: Throughput, Latency, Consumer Parallelism, and Future Growth.
 *
 * @throws Error if any metric is negative.
 */
declare const calculateOptimalPartitions: (metrics: ThroughputMetrics) => number;
/**
 * CLIENT EXTENSION: Adds partition analysis and calculation utilities to the Kafka Client.
 */
declare const partitionAnalysisExtension: () => KafkaExtension<{
  calculatePartitions: typeof calculateOptimalPartitions;
  checkPartitionSkew: (distribution: number[], skewThreshold?: number) => PartitionSkewResult;
}, "partitionAnalysis">;
//#endregion
//#region src/extensions/consumer-extension.d.ts
/**
 * Consumer Extension API
 */
interface ConsumerExtensionAPI {
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
  consume<TSchema extends ZodType>(groupId: string, topic: TopicConfig & {
    schema: TSchema;
  }, handler: (payload: z.infer<TSchema>, ctx: ConsumeHandlerContext) => Promise<void>, opts?: DLQOptions): Promise<Consumer$1>;
  consume(groupId: string, topic: TopicConfig, handler: (payload: kafkajs0.EachMessagePayload, ctx: ConsumeHandlerContext) => Promise<void>, opts?: DLQOptions): Promise<Consumer$1>;
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
declare const consumerExtension: () => KafkaExtension<ConsumerExtensionAPI, "consumer">;
//#endregion
//#region src/utils/brokers.d.ts
declare function resolveBrokersFromEnv(): string[];
//#endregion
export { BaseProducer, CONFIG_RESOURCE_TYPE_TOPIC, CircuitBreakerOptions, ConnectRetryOptions, ConsumeHandlerContext, Consumer, ConsumerConfig, ConsumerExtensionAPI, CreateDLQPublisherOptions, CreateKafkaClientConfig, CreatePublisherOptions, DEFAULT_OPTIONS, DLQContext, DLQMessageParts, DLQOptions, EventPublisher, GatwechKafkaExtensions, GenericKafkaProducer, ISerializer, JsonSerializer, KafkaClient, type KafkaClientConfig, KafkaClientOptions, KafkaExtension, KafkaMetrics, KeyDeriver, Logger, ModularKafkaClient, type PartitionKeyFn, PartitionSkewResult, type Producer, ReadyToPublishMessage, ReconnectionOptions, ResilienceOptions, RetryOptions, TKafkaMessage, ThroughputMetrics, type TopicConfig, TopicConfigBuilder, type TopicConfigInput, TopicConfigSchema, TopicConfigShape, TopicDef, TopicMap, TopicProvisioningOptions, type ValidatedTopicConfig, WithMetadata, asTopicDefs, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, consumerExtension, createDLQPublisher, createEventPublisher, createKafkaClient, createMissingTopics, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, safeDescribeConfigs, toConfigEntries, topicBuilder, topicsToArray, updatePartitionCounts, updateTopicConfigs, validateTopics, waitForLeaders, withRetry };