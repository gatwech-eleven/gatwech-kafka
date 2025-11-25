export * from "./client";
export * from "./config";
export * from "./types";
export * from "./extensions";
export * from "./utils/brokers";

export type { Producer } from "kafkajs";
export type { KafkaClientConfig } from "./client/core/create-client";

export { createEventPublisher } from "./client/producers/event.factory";
export { createServicePublisherFactory } from "./client/producers/service-publisher-factory";
export { createPublisher } from "./client/producers/event.factory";

export type { PartitionKeyFn } from "./client/producers/generic-producer";

// Export topic configuration API
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
export type {
  TopicConfig,
  TopicConfigInput,
  ValidatedTopicConfig,
} from "./config/topics/topic-config-schema";
