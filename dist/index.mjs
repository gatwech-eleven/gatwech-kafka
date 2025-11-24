import { n as logger, t as initializeKafkaTopics } from "./ensure-topics-Mazqk00P.mjs";
import { t as JsonSerializer } from "./types-Ca2NNx9a.mjs";
import { a as createPublisher, c as KafkaClient, i as createEventPublisher, l as connectWithRetry, n as createDLQPublisher, o as GenericKafkaProducer, r as createServicePublisherFactory, s as createKafkaClient, t as consumeWithRetryAndDLQ, u as BaseProducer } from "./client-Cw9aP7WN.mjs";
import "./utility-types-jjjEc54g.mjs";
import { a as topicsToArray, i as createTopicConfig, n as topicBuilder, o as TopicConfigSchema, r as createSingleTopicConfig, t as TopicConfigBuilder } from "./topics-BMuCBlzd.mjs";

//#region src/extensions/partition-analysis.ts
const DEFAULT_SKEW_THRESHOLD = 1.5;
/**
* Calculates the optimal number of partitions based on the 4-Pillar Framework
* derived from the video: Throughput, Latency, Consumer Parallelism, and Future Growth.
* 
* @throws Error if any metric is negative.
*/
const calculateOptimalPartitions = (metrics) => {
	const { targetThroughputMBps: T, producerPerPartitionMBps: P, consumerPerPartitionMBps: C, desiredParallelism, growthFactor } = metrics;
	if (T < 0 || P < 0 || C < 0 || desiredParallelism < 0 || growthFactor < 0) throw new Error("Throughput metrics cannot be negative.");
	const producerMin = P > 0 ? T / P : Infinity;
	const consumerMin = C > 0 ? T / C : Infinity;
	let throughputBasedMin = 0;
	if (P === 0 && C === 0 && T > 0) throughputBasedMin = Infinity;
	else if (P === 0 && C === 0 && T === 0) throughputBasedMin = 0;
	else throughputBasedMin = Math.ceil(Math.max(producerMin, consumerMin));
	if (!isFinite(throughputBasedMin)) {
		if (T > 0 && (P <= 0 || C <= 0)) {}
	}
	const parallelismTarget = desiredParallelism;
	const finalRequiredPartitions = Math.max(throughputBasedMin, parallelismTarget);
	const finalPartitionCount = Math.ceil(finalRequiredPartitions * growthFactor);
	return Math.max(1, finalPartitionCount);
};
/**
* CLIENT EXTENSION: Adds partition analysis and calculation utilities to the Kafka Client.
*/
const partitionAnalysisExtension = () => ({
	name: "partitionAnalysis",
	setup: (client) => {
		logger.info(`[Extension] Installing partition analysis client...`);
		return {
			calculatePartitions: calculateOptimalPartitions,
			checkPartitionSkew: (distribution, skewThreshold = DEFAULT_SKEW_THRESHOLD) => {
				if (skewThreshold <= 1) throw new Error("skewThreshold must be greater than 1.0");
				if (distribution.some((v) => v < 0)) throw new Error("Distribution cannot contain negative values");
				if (distribution.length === 0) return {
					status: "BALANCED",
					hotPartitions: []
				};
				const avgLoad = distribution.reduce((a, b) => a + b, 0) / distribution.length;
				const hotPartitions = distribution.map((load, index) => {
					if (load >= avgLoad * skewThreshold) return `Partition ${index} is 'Hot' (Load: ${load.toFixed(2)}, Avg: ${avgLoad.toFixed(2)})`;
					return null;
				}).filter((item) => item !== null);
				if (hotPartitions.length > 0) {
					logger.warn(`[PartitionSkew] Detected ${hotPartitions.length} hot partition(s).`);
					return {
						status: "SKEWED",
						hotPartitions
					};
				}
				return {
					status: "BALANCED",
					hotPartitions: []
				};
			}
		};
	}
});

//#endregion
//#region src/utils/brokers.ts
function resolveBrokersFromEnv() {
	const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
	if (raw && raw.trim().length > 0) return raw.split(",").map((s) => s.trim()).filter(Boolean);
	if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") return ["kafka:9092"];
	return ["localhost:9092"];
}

//#endregion
export { BaseProducer, GenericKafkaProducer, JsonSerializer, KafkaClient, TopicConfigBuilder, TopicConfigSchema, calculateOptimalPartitions, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createKafkaClient, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, initializeKafkaTopics, partitionAnalysisExtension, resolveBrokersFromEnv, topicBuilder, topicsToArray };