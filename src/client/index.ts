/** 
 * Client module exports
 * Provides core Kafka client functionality, producers, and consumers
 */

// ============================================================================
// Core Client
// ============================================================================

export { KafkaClient } from "./core/kafka";
export { createKafkaClient } from "./core/create-client";
export { connectWithRetry } from "./core/connect-with-retry";
export { DEFAULT_OPTIONS } from "./core/default-options";

export type {
  KafkaClientConfig,
  CreateKafkaClientConfig,
  ModularKafkaClient,
  KafkaExtension,
  GatwechKafkaExtensions,
} from "./core/create-client";

export type {
  ConnectRetryOptions,
} from "./core/connect-with-retry";

// ============================================================================
// Producers
// ============================================================================

export { BaseProducer } from "./producers/base";
export { createEventPublisher, createPublisher } from "./producers/event.factory";
export { GenericKafkaProducer } from "./producers/generic-producer";
export { createServicePublisherFactory } from "./producers/service-publisher-factory";

export type {
  CreatePublisherOptions,
} from "./producers/event.factory";

export type {
  EventPublisher,
} from "./producers/service-publisher-factory";

export type {
  PartitionKeyFn,
  KeyDeriver,
  ReadyToPublishMessage,
} from "./producers/generic-producer";

// ============================================================================
// Consumers
// ============================================================================

export { consumeWithRetryAndDLQ } from "./consumers/consume-with-dlq";
export { createDLQPublisher } from "./consumers/dlq.factory";

export type {
  ConsumeHandlerContext,
} from "./consumers/consume-with-dlq";

export type {
  DLQContext,
  DLQMessageParts,
  CreateDLQPublisherOptions,
} from "./consumers/dlq.factory";
