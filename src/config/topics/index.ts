/**
 * Topic configuration module
 * Provides utilities for defining, creating, and managing Kafka topics
 */

// Topic Definition API

export {
  createTopicConfig,
  createSingleTopicConfig,
  topicsToArray,
} from "./define-topics";

export {
  TopicConfigBuilder,
  topicBuilder,
} from "./topic-builder";

// Topic Management

export {
  createMissingTopics,
} from "./create-topics";

export {
  updatePartitionCounts,
} from "./update-partitions";

export {
  updateTopicConfigs,
} from "./update-configs";

export {
  asTopicDefs,
  validateTopics,
  toConfigEntries,
  safeDescribeConfigs,
  waitForLeaders,
  withRetry,
  CONFIG_RESOURCE_TYPE_TOPIC,
} from "./utils";

// Type Exports

export type {
  TopicConfig,
  TopicConfigInput,
  ValidatedTopicConfig,
} from "./topic-config-schema";

export type {
  TopicConfigShape,
  TopicDef,
  TopicMap,
} from "./utils";
