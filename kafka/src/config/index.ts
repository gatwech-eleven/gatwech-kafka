/**
 * Configuration module
 * Exports topic configuration and initialization utilities
 */

// Re-export all topic configuration utilities
export {
	createTopicConfig,
	createSingleTopicConfig,
	topicsToArray,
	TopicConfigBuilder,
	topicBuilder,
	createMissingTopics,
	updatePartitionCounts,
	updateTopicConfigs,
	asTopicDefs,
	validateTopics,
	toConfigEntries,
	safeDescribeConfigs,
	waitForLeaders,
	withRetry,
	CONFIG_RESOURCE_TYPE_TOPIC,
} from "./topics";

export type {
	TopicConfig,
	TopicConfigInput,
	ValidatedTopicConfig,
	TopicConfigShape,
	TopicDef,
	TopicMap,
} from "./topics";

// Topic initialization
export { initializeKafkaTopics } from "./initialize.topics";
