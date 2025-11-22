import { t as __export } from "./chunk-vZJVYSGk.mjs";
import { t as logger } from "./logger-BCcyae9U.mjs";
import { t as JsonSerializer } from "./types-BaLVca0q.mjs";
import { a as createEventPublisher, c as KafkaClient, i as connectWithRetry, l as BaseProducer, n as consumeWithRetryAndDLQ, o as createPublisher, r as createDLQPublisher, s as GenericKafkaProducer, t as createServicePublisherFactory, u as KafkaError } from "./client-B9oZkdRV.mjs";
import { r as BaseProducerSchema } from "./dlq.schema-Du3UyPGZ.mjs";
import { a as orderTopics, c as schemas_exports, i as paymentTopics, n as notificationTopics, o as chatTopics, r as userTopics, s as TopicConfigSchema, t as topics } from "./topics-2Tb-wREt.mjs";
import { t as ensureTopicsWithConfig } from "./ensure-topics-ByMtgJ2A.mjs";
import { z } from "@gatwech/utils";

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
export { BaseProducer, BaseProducerSchema, GenericKafkaProducer, JsonSerializer, KafkaClient, TopicConfigSchema, chatTopics, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createPublisher, createServicePublisherFactory, ensureTopicsWithConfig, notificationTopics, orderTopics, paymentTopics, validate_and_send_exports as producer, resolveBrokersFromEnv, schemas_exports as schemas, topics, userTopics, validation_exports as validation };