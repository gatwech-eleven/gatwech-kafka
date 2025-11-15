//#region rolldown:runtime
var __defProp = Object.defineProperty;
var __export = (all) => {
	let target = {};
	for (var name in all) __defProp(target, name, {
		get: all[name],
		enumerable: true
	});
	return target;
};

//#endregion
const require_logger = require('./logger-BhqH895u.cjs');
const require_types = require('./types-QqXxlQBy.cjs');
const require_client = require('./client-C5B-WBjq.cjs');
const require_topics = require('./topics-B2hfGQbN.cjs');
const require_ensure_topics = require('./ensure-topics-BBc_ly1C.cjs');
let __gatwech_utils = require("@gatwech/utils");
let __gatwech_utils_zod = require("@gatwech/utils/zod");

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
	const tree = __gatwech_utils.z.treeifyError(res.error);
	require_logger.logger.warn("Kafka message validation failed", {
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
		const tree = __gatwech_utils.z.treeifyError(res.error);
		require_logger.logger.warn("Producer validation failed; not sending message", {
			topic,
			errors: tree
		});
		throw new require_client.KafkaError("Validation failed for message", res.error);
	}
	return client.sendMessage(topic, message);
}
/** Validate and send multiple messages. Any invalid message will abort sending. */
async function validateAndSendMany(client, topic, schema, messages) {
	for (const msg of messages) {
		const res = schema.safeParse(msg.value);
		if (!res.success) {
			const tree = __gatwech_utils.z.treeifyError(res.error);
			require_logger.logger.warn("Producer batch validation failed; not sending batch", {
				topic,
				errors: tree
			});
			throw new require_client.KafkaError("Validation failed for batch message", res.error);
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
const UserRegisteredSchema = __gatwech_utils_zod.z.object({
	id: __gatwech_utils_zod.z.string(),
	email: __gatwech_utils_zod.z.email(),
	name: __gatwech_utils_zod.z.string().optional(),
	otp: __gatwech_utils_zod.z.string().optional(),
	timestamp: __gatwech_utils_zod.z.date().default(() => /* @__PURE__ */ new Date()),
	metadata: __gatwech_utils_zod.z.object({
		correlationId: __gatwech_utils_zod.z.string().optional(),
		source: __gatwech_utils_zod.z.string().optional()
	}).optional()
});
const ResetPasswordRequestEmailSchema = __gatwech_utils_zod.z.object({
	userId: __gatwech_utils_zod.z.uuid().optional(),
	identifier: __gatwech_utils_zod.z.literal("email"),
	email: __gatwech_utils_zod.z.email(),
	token: __gatwech_utils_zod.z.string(),
	timestamp: __gatwech_utils_zod.z.date().default(() => /* @__PURE__ */ new Date()),
	metadata: __gatwech_utils_zod.z.object({
		correlationId: __gatwech_utils_zod.z.string().optional(),
		source: __gatwech_utils_zod.z.string().optional()
	}).optional()
});
const ResetPasswordRequestPhoneSchema = __gatwech_utils_zod.z.object({
	userId: __gatwech_utils_zod.z.uuid().optional(),
	identifier: __gatwech_utils_zod.z.literal("phone"),
	phoneNumber: __gatwech_utils_zod.z.string(),
	otp: __gatwech_utils_zod.z.string().optional(),
	token: __gatwech_utils_zod.z.string().optional(),
	timestamp: __gatwech_utils_zod.z.date().default(() => /* @__PURE__ */ new Date()),
	metadata: __gatwech_utils_zod.z.object({
		correlationId: __gatwech_utils_zod.z.string().optional(),
		source: __gatwech_utils_zod.z.string().optional()
	}).optional()
});
const ResetPasswordRequestUnionSchema = __gatwech_utils_zod.z.union([ResetPasswordRequestEmailSchema, ResetPasswordRequestPhoneSchema]);
var user_schemas_default = {
	UserRegisteredSchema,
	ResetPasswordRequestEmailSchema,
	ResetPasswordRequestPhoneSchema,
	ResetPasswordRequestUnionSchema
};

//#endregion
//#region src/schemas/notification-schemas.ts
const NotificationEmailSentSchema = __gatwech_utils_zod.z.object({
	to: __gatwech_utils_zod.z.string().email(),
	subject: __gatwech_utils_zod.z.string(),
	body: __gatwech_utils_zod.z.string(),
	timestamp: __gatwech_utils_zod.z.date().default(() => /* @__PURE__ */ new Date()),
	metadata: __gatwech_utils_zod.z.object({
		correlationId: __gatwech_utils_zod.z.string().optional(),
		source: __gatwech_utils_zod.z.string().optional()
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
const BaseProducerSchema = __gatwech_utils_zod.z.object({
	timestamp: __gatwech_utils_zod.z.date().optional().default(() => /* @__PURE__ */ new Date()),
	metadata: __gatwech_utils_zod.z.any().optional()
});

//#endregion
exports.BaseProducer = require_client.BaseProducer;
exports.BaseProducerSchema = BaseProducerSchema;
exports.JsonSerializer = require_types.JsonSerializer;
exports.KafkaClient = require_client.KafkaClient;
exports.KafkaError = require_client.KafkaError;
exports.chatTopics = require_topics.chatTopics;
exports.connectWithRetry = require_client.connectWithRetry;
exports.createEventPublisher = require_client.createEventPublisher;
exports.ensureTopicsWithConfig = require_ensure_topics.ensureTopicsWithConfig;
exports.notificationTopics = require_topics.notificationTopics;
exports.orderTopics = require_topics.orderTopics;
exports.paymentTopics = require_topics.paymentTopics;
Object.defineProperty(exports, 'producer', {
  enumerable: true,
  get: function () {
    return validate_and_send_exports;
  }
});
exports.resolveBrokersFromEnv = resolveBrokersFromEnv;
Object.defineProperty(exports, 'schemas', {
  enumerable: true,
  get: function () {
    return schemas_exports;
  }
});
exports.topics = require_topics.topics;
exports.userTopics = require_topics.userTopics;
Object.defineProperty(exports, 'validation', {
  enumerable: true,
  get: function () {
    return validation_exports;
  }
});