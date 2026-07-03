/**
 * Type definitions module
 * Exports all TypeScript types and interfaces for the Kafka client
 */

// Core Kafka Types

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
} from "./kafka";

// Runtime class export
export {
  JsonSerializer,
} from "./kafka";


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
} from "./utility-types";

// KafkaJS Type Re-exports

export type { Producer, Consumer, ConsumerConfig } from "kafkajs";