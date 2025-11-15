import { t as __export } from "./chunk-vZJVYSGk.mjs";
import { t as logger } from "./logger-BCcyae9U.mjs";
import { t as JsonSerializer } from "./types-BaLVca0q.mjs";
import { a as KafkaError, i as KafkaClient, n as createEventPublisher, r as BaseProducer, t as connectWithRetry } from "./client-D7A2EpKK.mjs";
import { a as paymentTopics, i as notificationTopics, n as orderTopics, o as userTopics, r as chatTopics, t as topics } from "./topics-DSardRd6.mjs";
import { t as ensureTopicsWithConfig } from "./ensure-topics--SOf6Tr8.mjs";
import { z } from "@gatwech/utils";
import { z as z$1 } from "@gatwech/utils/zod";

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
async function validateAndSend(client, topic, schema, message) {
	const res = schema.safeParse(message.value);
	if (!res.success) {
		const tree = z.treeifyError(res.error);
		logger.warn("Producer validation failed; not sending message", {
			topic,
			errors: tree
		});
		throw new KafkaError("Validation failed for message", res.error);
	}
	return client.sendMessage(topic, message);
}
/** Validate and send multiple messages. Any invalid message will abort sending. */
async function validateAndSendMany(client, topic, schema, messages) {
	for (const msg of messages) {
		const res = schema.safeParse(msg.value);
		if (!res.success) {
			const tree = z.treeifyError(res.error);
			logger.warn("Producer batch validation failed; not sending batch", {
				topic,
				errors: tree
			});
			throw new KafkaError("Validation failed for batch message", res.error);
		}
	}
	return client.sendMessages(topic, messages);
}
var validate_and_send_default = {
	validateAndSend,
	validateAndSendMany
};

//#endregion
//#region src/schemas/user-schemas.ts
const UserRegisteredSchema = z$1.object({
	id: z$1.string(),
	email: z$1.email(),
	name: z$1.string().optional(),
	otp: z$1.string().optional(),
	timestamp: z$1.date().default(() => /* @__PURE__ */ new Date()),
	metadata: z$1.object({
		correlationId: z$1.string().optional(),
		source: z$1.string().optional()
	}).optional()
});
const ResetPasswordRequestEmailSchema = z$1.object({
	userId: z$1.uuid().optional(),
	identifier: z$1.literal("email"),
	email: z$1.email(),
	token: z$1.string(),
	timestamp: z$1.date().default(() => /* @__PURE__ */ new Date()),
	metadata: z$1.object({
		correlationId: z$1.string().optional(),
		source: z$1.string().optional()
	}).optional()
});
const ResetPasswordRequestPhoneSchema = z$1.object({
	userId: z$1.uuid().optional(),
	identifier: z$1.literal("phone"),
	phoneNumber: z$1.string(),
	otp: z$1.string().optional(),
	token: z$1.string().optional(),
	timestamp: z$1.date().default(() => /* @__PURE__ */ new Date()),
	metadata: z$1.object({
		correlationId: z$1.string().optional(),
		source: z$1.string().optional()
	}).optional()
});
const ResetPasswordRequestUnionSchema = z$1.union([ResetPasswordRequestEmailSchema, ResetPasswordRequestPhoneSchema]);
var user_schemas_default = {
	UserRegisteredSchema,
	ResetPasswordRequestEmailSchema,
	ResetPasswordRequestPhoneSchema,
	ResetPasswordRequestUnionSchema
};

//#endregion
//#region src/schemas/notification-schemas.ts
const NotificationEmailSentSchema = z$1.object({
	to: z$1.string().email(),
	subject: z$1.string(),
	body: z$1.string(),
	timestamp: z$1.date().default(() => /* @__PURE__ */ new Date()),
	metadata: z$1.object({
		correlationId: z$1.string().optional(),
		source: z$1.string().optional()
	}).optional()
});
var notification_schemas_default = { NotificationEmailSentSchema };

//#endregion
//#region src/schemas/index.ts
var schemas_exports = /* @__PURE__ */ __export({
	NotificationEmailSentSchema: () => NotificationEmailSentSchema,
	ResetPasswordRequestEmailSchema: () => ResetPasswordRequestEmailSchema,
	ResetPasswordRequestPhoneSchema: () => ResetPasswordRequestPhoneSchema,
	ResetPasswordRequestUnionSchema: () => ResetPasswordRequestUnionSchema,
	UserRegisteredSchema: () => UserRegisteredSchema,
	notificationSchemas: () => notification_schemas_default,
	userSchemas: () => user_schemas_default
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
//#region src/schemas/base.schema.ts
const BaseProducerSchema = z$1.object({
	timestamp: z$1.date().optional().default(() => /* @__PURE__ */ new Date()),
	metadata: z$1.any().optional()
});

//#endregion
export { BaseProducer, BaseProducerSchema, JsonSerializer, KafkaClient, KafkaError, chatTopics, connectWithRetry, createEventPublisher, ensureTopicsWithConfig, notificationTopics, orderTopics, paymentTopics, validate_and_send_exports as producer, resolveBrokersFromEnv, schemas_exports as schemas, topics, userTopics, validation_exports as validation };