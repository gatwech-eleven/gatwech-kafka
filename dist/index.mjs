import { t as __export } from "./chunk-vZJVYSGk.mjs";
import { t as logger } from "./logger-BCcyae9U.mjs";
import { t as JsonSerializer } from "./types-BaLVca0q.mjs";
import { a as createPublisher, c as createKafkaClient, d as KafkaError, i as createEventPublisher, l as KafkaClient, n as createDLQPublisher, o as GenericKafkaProducer, r as createServicePublisherFactory, s as connectWithRetry, t as consumeWithRetryAndDLQ, u as BaseProducer } from "./client-CTp2pfDd.mjs";
import { r as BaseProducerSchema } from "./dlq.schema-K42xDTcu.mjs";
import { a as createTopicConfig, c as topicsToArray, d as paymentTopics, f as orderTopics, h as schemas_exports, i as createSingleTopicConfig, l as notificationTopics, m as TopicConfigSchema, n as TopicConfigBuilder, o as defineTopic, p as chatTopics, r as topicBuilder, s as defineTopics, t as topics, u as userTopics } from "./topics-DLYMu4hC.mjs";
import { n as initializeKafkaTopics, t as ensureTopicsWithConfig } from "./ensure-topics-CG3Bl2tc.mjs";
import { z } from "@gatwech/utils";

//#region src/extensions/topic-provisioning.ts
/**
* Extension that adds topic provisioning capabilities to the Kafka client.
*
* @param topics - Map of topic configurations (from createTopicConfig)
* @param options - Provisioning options
*/
function topicProvisioningExtension(topics$1, options = {}) {
	return {
		name: "topicProvisioning",
		setup: (client) => {
			return { provisionTopics: async () => {
				await initializeKafkaTopics(client.getAdmin(), {
					topics: topics$1,
					partitions: options.partitions,
					replicationFactor: options.replicationFactor,
					waitForLeadersMs: options.waitForLeadersMs,
					includeBuiltIn: options.includeBuiltIn
				});
			} };
		}
	};
}

//#endregion
//#region src/extensions/health-check.ts
/**
* Extension that provides health check functionality.
*
* While the base client has a healthCheck method, this extension allows
* for more consistent access patterns in the modular architecture.
*/
function healthCheckExtension() {
	return {
		name: "healthCheck",
		setup: (client) => {
			const originalHealthCheck = client.healthCheck.bind(client);
			return { healthCheck: async () => {
				return originalHealthCheck();
			} };
		}
	};
}

//#endregion
//#region src/extensions/partition-calc.ts
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
const partitionAnalysisClient = () => ({
	name: "PartitionAnalysisClient",
	setup: (client) => {
		console.log(`[Extension] Installing partition analysis client...`);
		return {
			calculatePartitions: calculateOptimalPartitions,
			checkPartitionSkew: (distribution, skewThreshold = 1.5) => {
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
					console.warn(`[PartitionSkew] Detected ${hotPartitions.length} hot partition(s).`);
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
//#region src/validation/index.ts
var validation_exports = /* @__PURE__ */ __export({
	default: () => validation_default,
	validateMessage: () => validateMessage
});
function validateMessage(schema, value, context) {
	const res = schema.safeParse(value);
	if (res.success) return {
		success: true,
		data: res.data
	};
	const tree = z.treeifyError(res.error);
	logger.warn("Kafka message validation failed", {
		...context || {},
		errors: tree
	});
	return {
		success: false,
		error: tree
	};
}
var validation_default = { validateMessage };

//#endregion
//#region src/producer/validate-and-send.ts
var validate_and_send_exports = /* @__PURE__ */ __export({
	default: () => validate_and_send_default,
	validateAndSend: () => validateAndSend,
	validateAndSendMany: () => validateAndSendMany
});
/**
* @deprecated Use `createServicePublisher` (preferred) or `createEventPublisher`.
* This helper performs schema validation then sends via a generic producer,
* but the service/event factory APIs provide richer metadata injection and clearer contracts.
*/
async function validateAndSend(client, topic, message) {
	if (!topic.schema) {
		logger.warn("No schema defined for topic; not validating message", { topic: topic.name });
		return client.sendMessage(topic.name, message);
	}
	const parsed = topic.schema.safeParse(message.value);
	if (!parsed.success) {
		const tree = z.treeifyError(parsed.error);
		logger.warn("Producer validation failed; not sending message", {
			topic: topic.name,
			errors: tree
		});
		throw new KafkaError("Validation failed for message", parsed.error);
	}
	const producer = client.getProducer();
	let currentKey = message.key;
	const generic = new GenericKafkaProducer(producer, topic.name, () => currentKey);
	const payloadWithTimestamp = {
		...parsed.data,
		timestamp: parsed.data?.timestamp || Date.now(),
		metadata: parsed.data?.metadata
	};
	await generic.send(payloadWithTimestamp);
}
/** Validate and send multiple messages. Any invalid message will abort sending. */
/**
* @deprecated Use `createServicePublisher` (preferred) or `createEventPublisher`.
* Validates each message and publishes as a batch. Prefer defining per-topic publishers
* with factories for stronger typing, metadata enrichment, and observability.
*/
async function validateAndSendMany(client, topic, messages) {
	if (!topic.schema) {
		logger.warn("No schema defined for topic; not validating messages", { topic: topic.name });
		return client.sendMessages(topic.name, messages);
	}
	const schema = topic.schema;
	const producer = client.getProducer();
	let currentKey = "";
	const generic = new GenericKafkaProducer(producer, topic.name, () => currentKey);
	for (const msg of messages) {
		const res = schema.safeParse(msg.value);
		logger.info("Batch validation result", {
			key: msg.key,
			success: res.success,
			valueKeys: Object.keys(msg.value || {})
		});
		if (!res.success) {
			logger.warn("Producer batch validation failed; not sending batch", { topic: topic.name });
			let tree;
			try {
				tree = z.treeifyError(res.error);
				logger.warn("Batch validation errors", {
					topic: topic.name,
					errors: tree
				});
			} catch (e) {
				logger.warn("treeifyError failed", {
					topic: topic.name,
					error: e?.message
				});
			}
			throw new KafkaError("Validation failed for batch message", res.error);
		}
	}
	const batchItems = messages.map((msg) => {
		const enriched = {
			...msg.value,
			timestamp: msg.value?.timestamp || Date.now(),
			metadata: msg.value?.metadata
		};
		return {
			key: msg.key,
			payload: enriched
		};
	});
	await generic.sendBatch(batchItems);
}
var validate_and_send_default = {
	validateAndSend,
	validateAndSendMany
};

//#endregion
//#region src/utils/brokers.ts
function resolveBrokersFromEnv() {
	const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
	if (raw && raw.trim().length > 0) return raw.split(",").map((s) => s.trim()).filter(Boolean);
	if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") return ["kafka:9092"];
	return ["localhost:9092"];
}

//#endregion
export { BaseProducer, BaseProducerSchema, GenericKafkaProducer, JsonSerializer, KafkaClient, TopicConfigBuilder, TopicConfigSchema, calculateOptimalPartitions, chatTopics, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createKafkaClient, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, defineTopic, defineTopics, ensureTopicsWithConfig, healthCheckExtension, initializeKafkaTopics, notificationTopics, orderTopics, partitionAnalysisClient, paymentTopics, validate_and_send_exports as producer, resolveBrokersFromEnv, schemas_exports as schemas, topicBuilder, topicProvisioningExtension, topics, topicsToArray, userTopics, validation_exports as validation };