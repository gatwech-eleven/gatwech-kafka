import "./logger-CSM-3OsQ.mjs";
import { a as BaseProducer, c as createDLQPublisher, i as GenericKafkaProducer, l as connectWithRetry, n as createEventPublisher, o as createKafkaClient, r as createPublisher, s as consumeWithRetryAndDLQ, t as createServicePublisherFactory, u as DEFAULT_OPTIONS } from "./service-publisher-factory-CbYSe4uK.mjs";
import { a as createMissingTopics, c as safeDescribeConfigs, d as waitForLeaders, f as withRetry, i as updateTopicConfigs, l as toConfigEntries, o as CONFIG_RESOURCE_TYPE_TOPIC, r as updatePartitionCounts, s as asTopicDefs, u as validateTopics } from "./utility-types-D_cu1j2I.mjs";
import { t as initializeKafkaTopics } from "./initialize.topics-CXOrVpmL.mjs";
import { t as JsonSerializer } from "./types-B7ehgEUe.mjs";
import { a as topicsToArray, i as createTopicConfig, n as topicBuilder, r as createSingleTopicConfig, t as TopicConfigBuilder } from "./topic-builder-DoLGJ9a2.mjs";
import { n as partitionAnalysisExtension, t as calculateOptimalPartitions } from "./partition-analysis-D-F5Pa-m.mjs";

//#region src/utils/brokers.ts
function resolveBrokersFromEnv() {
	const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
	if (raw && raw.trim().length > 0) return raw.split(",").map((s) => s.trim()).filter(Boolean);
	if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") return ["kafka:9092"];
	return ["localhost:9092"];
}

//#endregion
export { BaseProducer, CONFIG_RESOURCE_TYPE_TOPIC, DEFAULT_OPTIONS, GenericKafkaProducer, JsonSerializer, TopicConfigBuilder, asTopicDefs, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createKafkaClient, createMissingTopics, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, safeDescribeConfigs, toConfigEntries, topicBuilder, topicsToArray, updatePartitionCounts, updateTopicConfigs, validateTopics, waitForLeaders, withRetry };