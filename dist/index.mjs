import { t as __export } from "./chunk-vZJVYSGk.mjs";
import { t as logger } from "./logger-BCcyae9U.mjs";
import { t as JsonSerializer } from "./types-BaLVca0q.mjs";
import { a as KafkaClient, i as BaseProducer, n as connectWithRetry, o as KafkaError, r as createEventPublisher, t as consumeWithRetryAndDLQ } from "./client-CEgJz8dZ.mjs";
import { n as BaseProducerSchema } from "./dlq.schema-DdQ9wUZ4.mjs";
import { a as orderTopics, c as schemas_exports, i as paymentTopics, n as notificationTopics, o as chatTopics, r as userTopics, s as TopicConfigSchema, t as topics } from "./topics-Bb390ehd.mjs";
import { t as ensureTopicsWithConfig } from "./ensure-topics-BcAJqOsO.mjs";
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
/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
async function validateAndSend(client, topic, message) {
	if (!topic.schema) {
		logger.warn("No schema defined for topic; not validating message", { topic: topic.name });
		return client.sendMessage(topic.name, message);
	}
	const res = topic.schema.safeParse(message.value);
	if (!res.success) {
		const tree = z.treeifyError(res.error);
		logger.warn("Producer validation failed; not sending message", {
			topic: topic.name,
			errors: tree
		});
		throw new KafkaError("Validation failed for message", res.error);
	}
	return client.sendMessage(topic.name, message);
}
/** Validate and send multiple messages. Any invalid message will abort sending. */
async function validateAndSendMany(client, topic, messages) {
	if (!topic.schema) {
		logger.warn("No schema defined for topic; not validating messages", { topic: topic.name });
		return client.sendMessages(topic.name, messages);
	}
	const schema = topic.schema;
	for (const msg of messages) {
		const res = schema.safeParse(msg.value);
		if (!res.success) {
			const tree = z.treeifyError(res.error);
			logger.warn("Producer batch validation failed; not sending batch", {
				topic: topic.name,
				errors: tree
			});
			throw new KafkaError("Validation failed for batch message", res.error);
		}
	}
	return client.sendMessages(topic.name, messages);
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
export { BaseProducer, BaseProducerSchema, JsonSerializer, KafkaClient, KafkaError, TopicConfigSchema, chatTopics, connectWithRetry, consumeWithRetryAndDLQ, createEventPublisher, ensureTopicsWithConfig, notificationTopics, orderTopics, paymentTopics, validate_and_send_exports as producer, resolveBrokersFromEnv, schemas_exports as schemas, topics, userTopics, validation_exports as validation };