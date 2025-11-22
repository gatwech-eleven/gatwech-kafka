export * from "./client";
export * from "./config";
export * from "./types";
export * from "./extensions";
export * as validation from "./validation";
export * as producer from "./producer/validate-and-send";
export * as schemas from "./schemas";
export * from "./utils/brokers";

export type { Producer } from "kafkajs";

export { createEventPublisher } from "./client/producers/event.factory";
export { createServicePublisherFactory } from "./client/producers/service-publisher-factory";
export { createPublisher } from "./client/producers/event.factory";
export { BaseProducerSchema } from "./schemas/base.schema";

export type { PartitionKeyFn } from "./client/producers/generic-producer";

// Export new topic configuration API
export {
  createTopicConfig,
  createSingleTopicConfig,
  topicsToArray,
  defineTopics, // Deprecated
  defineTopic, // Deprecated
} from "./config/topics/define-topics";
export {
  TopicConfigBuilder,
  topicBuilder,
} from "./config/topics/topic-builder";
export {
  initializeKafkaTopics,
  ensureTopicsWithConfig, // Deprecated but kept for backward compatibility
} from "./config/ensure-topics";
export type {
  TopicConfig,
  TopicConfigInput,
  ValidatedTopicConfig,
} from "./config/topics/topic-config-schema";


