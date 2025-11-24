import "./utility-types-De8KORrn.mjs";
import { a as JsonSerializer, c as Logger, d as TKafkaMessage, f as TopicProvisioningOptions, i as ISerializer, n as ConsumerConfig, o as KafkaClientOptions, p as WithMetadata, r as DLQOptions, s as KafkaMetrics, t as Consumer, u as ResilienceOptions } from "./index-DU6vcUpS.mjs";
import { C as KafkaClient, S as createKafkaClient, _ as connectWithRetry, a as ConsumeHandlerContext, b as KafkaExtension, c as createServicePublisherFactory, d as createPublisher, f as GenericKafkaProducer, g as ConnectRetryOptions, h as ReadyToPublishMessage, i as createDLQPublisher, l as CreatePublisherOptions, m as PartitionKeyFn, n as DLQContext, o as consumeWithRetryAndDLQ, p as KeyDeriver, r as DLQMessageParts, s as EventPublisher, t as CreateDLQPublisherOptions, u as createEventPublisher, v as CreateKafkaClientConfig, w as BaseProducer, x as ModularKafkaClient, y as GatwechKafkaExtensions } from "./index-ChwNLFQZ.mjs";
import { i as ValidatedTopicConfig, n as TopicConfigInput, r as TopicConfigSchema, t as TopicConfig } from "./topic-config-schema-CF6nl8lv.mjs";
import { n as initializeKafkaTopics, t as TopicMap } from "./ensure-topics-CNaFB69u.mjs";
import { a as topicsToArray, i as createTopicConfig, n as topicBuilder, r as createSingleTopicConfig, t as TopicConfigBuilder } from "./index-Cv4mKtT2.mjs";
import { Producer } from "kafkajs";

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
//#region src/utils/brokers.d.ts
declare function resolveBrokersFromEnv(): string[];
//#endregion
export { BaseProducer, ConnectRetryOptions, ConsumeHandlerContext, Consumer, ConsumerConfig, CreateDLQPublisherOptions, CreateKafkaClientConfig, CreatePublisherOptions, DLQContext, DLQMessageParts, DLQOptions, EventPublisher, GatwechKafkaExtensions, GenericKafkaProducer, ISerializer, JsonSerializer, KafkaClient, KafkaClientOptions, KafkaExtension, KafkaMetrics, KeyDeriver, Logger, ModularKafkaClient, type PartitionKeyFn, PartitionSkewResult, type Producer, ReadyToPublishMessage, ResilienceOptions, TKafkaMessage, ThroughputMetrics, type TopicConfig, TopicConfigBuilder, type TopicConfigInput, TopicConfigSchema, TopicMap, TopicProvisioningOptions, type ValidatedTopicConfig, WithMetadata, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createKafkaClient, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, topicBuilder, topicsToArray };