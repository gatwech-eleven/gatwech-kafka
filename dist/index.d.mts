import "./utility-types-CCAClNOD.mjs";
import { a as JsonSerializer, c as Logger, d as TKafkaMessage, f as TopicProvisioningOptions, i as ISerializer, n as ConsumerConfig, o as KafkaClientOptions, p as WithMetadata, r as DLQOptions, s as KafkaMetrics, t as Consumer, u as ResilienceOptions } from "./index-BHn74P_a.mjs";
import { C as ModularKafkaClient, E as BaseProducer, S as KafkaExtension, T as KafkaClient, _ as ConnectRetryOptions, a as ConsumeHandlerContext, b as GatwechKafkaExtensions, c as createServicePublisherFactory, d as createPublisher, f as GenericKafkaProducer, g as DEFAULT_OPTIONS, h as ReadyToPublishMessage, i as createDLQPublisher, l as CreatePublisherOptions, m as PartitionKeyFn, n as DLQContext, o as consumeWithRetryAndDLQ, p as KeyDeriver, r as DLQMessageParts, s as EventPublisher, t as CreateDLQPublisherOptions, u as createEventPublisher, v as connectWithRetry, w as createKafkaClient, x as KafkaClientConfig, y as CreateKafkaClientConfig } from "./index-C0rhzP5x.mjs";
import { i as ValidatedTopicConfig, n as TopicConfigInput, r as TopicConfigSchema, t as TopicConfig } from "./topic-config-schema-Q63DD7qq.mjs";
import { n as initializeKafkaTopics, t as TopicMap } from "./ensure-topics-l1WZ1Zh0.mjs";
import { a as topicsToArray, i as createTopicConfig, n as topicBuilder, r as createSingleTopicConfig, t as TopicConfigBuilder } from "./index-CVBEaQD9.mjs";
import * as kafkajs0 from "kafkajs";
import { Consumer as Consumer$1, Producer } from "kafkajs";
import { ZodType, z } from "@gatwech/utils/zod";

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
export { BaseProducer, ConnectRetryOptions, ConsumeHandlerContext, Consumer, ConsumerConfig, ConsumerExtensionAPI, CreateDLQPublisherOptions, CreateKafkaClientConfig, CreatePublisherOptions, DEFAULT_OPTIONS, DLQContext, DLQMessageParts, DLQOptions, EventPublisher, GatwechKafkaExtensions, GenericKafkaProducer, ISerializer, JsonSerializer, KafkaClient, type KafkaClientConfig, KafkaClientOptions, KafkaExtension, KafkaMetrics, KeyDeriver, Logger, ModularKafkaClient, type PartitionKeyFn, PartitionSkewResult, type Producer, ReadyToPublishMessage, ResilienceOptions, TKafkaMessage, ThroughputMetrics, type TopicConfig, TopicConfigBuilder, type TopicConfigInput, TopicConfigSchema, TopicMap, TopicProvisioningOptions, type ValidatedTopicConfig, WithMetadata, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, consumerExtension, createDLQPublisher, createEventPublisher, createKafkaClient, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, topicBuilder, topicsToArray };