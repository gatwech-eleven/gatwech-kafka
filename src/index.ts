// ============================================================================
// Core Client Exports
// ============================================================================

// KafkaClient type is exported from create-client
export { createKafkaClient } from "./client/core/create-client";
export { connectWithRetry } from "./client/core/connect-with-retry";
export { DEFAULT_OPTIONS } from "./client/core/default-options";

export type {
  KafkaClient,
  KafkaClientConfig,
  CreateKafkaClientConfig,
  KafkaExtension,
  GatwechKafkaExtensions,
} from "./client/core/create-client";

export type {
  ConnectRetryOptions,
} from "./client/core/connect-with-retry";

// ============================================================================
// Producer Exports
// ============================================================================

export { createEventPublisher, createPublisher } from "./client/producers/event.factory";
export { createServicePublisherFactory } from "./client/producers/service-publisher-factory";
export { GenericKafkaProducer } from "./client/producers/generic-producer";
export { BaseProducer } from "./client/producers/base";

export type {
  CreatePublisherOptions,
} from "./client/producers/event.factory";

export type {
  EventPublisher,
} from "./client/producers/service-publisher-factory";

export type {
  PartitionKeyFn,
  KeyDeriver,
  ReadyToPublishMessage,
} from "./client/producers/generic-producer";

// ============================================================================
// Consumer Exports
// ============================================================================

export { consumeWithRetryAndDLQ } from "./client/consumers/consume-with-dlq";
export { createDLQPublisher } from "./client/consumers/dlq.factory";

export type {
  ConsumeHandlerContext,
} from "./client/consumers/consume-with-dlq";

export type {
  DLQContext,
  DLQMessageParts,
  CreateDLQPublisherOptions,
} from "./client/consumers/dlq.factory";

// ============================================================================
// Topic Configuration Exports
// ============================================================================

export {
  createTopicConfig,
  createSingleTopicConfig,
  topicsToArray,
} from "./config/topics/define-topics";

export {
  TopicConfigBuilder,
  topicBuilder,
} from "./config/topics/topic-builder";

export {
  initializeKafkaTopics,
} from "./config/initialize.topics";

export {
  createMissingTopics,
} from "./config/topics/create-topics";

export {
  updatePartitionCounts,
} from "./config/topics/update-partitions";

export {
  updateTopicConfigs,
} from "./config/topics/update-configs";

export {
  asTopicDefs,
  validateTopics,
  toConfigEntries,
  safeDescribeConfigs,
  waitForLeaders,
  CONFIG_RESOURCE_TYPE_TOPIC,
} from "./config/topics/utils";

export type {
  TopicConfig,
  TopicConfigInput,
  ValidatedTopicConfig,
} from "./config/topics/topic-config-schema";

export type {
  TopicConfigShape,
  TopicDef,
  TopicMap,
} from "./config/topics/utils";

// ============================================================================
// Extension Exports
// ============================================================================

export { partitionAnalysisExtension } from "./extensions/partition-analysis";

export type {
  PartitionSkewResult,
  ThroughputMetrics,
} from "./extensions/partition-analysis";

// ============================================================================
// Type Exports
// ============================================================================

export type {
  KafkaClientOptions,
  Logger,
  KafkaMetrics,
  ISerializer,
  TKafkaMessage,
  WithMetadata,
  DLQOptions,
  TopicProvisioningOptions,
  ResilienceOptions,
  RetryOptions,
  ReconnectionOptions,
  CircuitBreakerOptions,
} from "./types/kafka";

export {
  JsonSerializer,
} from "./types/kafka";

export type {
  SchemaConsumerPayload,
  RawConsumerPayload,
  ConsumerDlqOptions,
  RawTopic,
  AnyTopic,
} from "./types/consumer";

export type {
  DeepReadonly,
  StrictOmit,
  OptionalExceptFor,
  RequireKeys,
  Mutable,
  Brand,
  CorrelationId,
  ServiceSource,
  SchemaHash,
  TopicName,
  ConsumerGroupId,
} from "./types/utility-types";

// ============================================================================
// Utility Exports
// ============================================================================

export { resolveBrokersFromEnv } from "./utils/brokers";
export { withRetry } from "./config/topics/utils";
export { calculateOptimalPartitions } from "./extensions/partition-analysis";

// ============================================================================
// KafkaJS Re-exports (for convenience)
// Note: Consider importing directly from 'kafkajs' for better version control
// ============================================================================

export type { Producer, Consumer, ConsumerConfig } from "kafkajs";
