import { a as createMissingTopics, c as safeDescribeConfigs, d as waitForLeaders, f as withRetry, i as updateTopicConfigs, l as toConfigEntries, o as CONFIG_RESOURCE_TYPE_TOPIC, r as updatePartitionCounts, s as asTopicDefs, u as validateTopics } from "./utility-types-DaM3Tt4b.mjs";
import { a as createPublisher, c as createKafkaClient, d as DEFAULT_OPTIONS, i as createEventPublisher, l as KafkaClient, n as createDLQPublisher, o as GenericKafkaProducer, r as createServicePublisherFactory, s as BaseProducer, t as consumeWithRetryAndDLQ, u as connectWithRetry } from "./client-DXNF4tpg.mjs";
import { t as initializeKafkaTopics } from "./initialize.topics-CK3azKNJ.mjs";
import { t as JsonSerializer } from "./types-Bkfbo456.mjs";
import { a as topicsToArray, i as createTopicConfig, n as topicBuilder, r as createSingleTopicConfig, t as TopicConfigBuilder } from "./topic-builder-DC8ptbQg.mjs";
import { n as calculateOptimalPartitions, r as partitionAnalysisExtension, t as consumerExtension } from "./consumer-extension-BSzGHVQb.mjs";

//#region src/utils/brokers.ts
function resolveBrokersFromEnv() {
	const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
	if (raw && raw.trim().length > 0) return raw.split(",").map((s) => s.trim()).filter(Boolean);
	if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") return ["kafka:9092"];
	return ["localhost:9092"];
}

//#endregion
export { BaseProducer, CONFIG_RESOURCE_TYPE_TOPIC, DEFAULT_OPTIONS, GenericKafkaProducer, JsonSerializer, KafkaClient, TopicConfigBuilder, asTopicDefs, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, consumerExtension, createDLQPublisher, createEventPublisher, createKafkaClient, createMissingTopics, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, safeDescribeConfigs, toConfigEntries, topicBuilder, topicsToArray, updatePartitionCounts, updateTopicConfigs, validateTopics, waitForLeaders, withRetry };