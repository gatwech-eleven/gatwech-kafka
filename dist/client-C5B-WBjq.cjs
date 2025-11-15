const require_logger = require('./logger-BhqH895u.cjs');
const require_types = require('./types-QqXxlQBy.cjs');
let kafkajs = require("kafkajs");
let __gatwech_utils = require("@gatwech/utils");
let nanoid = require("nanoid");

//#region src/client/kafka-client.ts
var KafkaError = class KafkaError extends Error {
	constructor(message, cause, meta) {
		super(message);
		this.cause = cause;
		this.meta = meta;
		this.name = "KafkaError";
		Object.setPrototypeOf(this, KafkaError.prototype);
	}
};
const DEFAULT_OPTIONS = {
	allowAutoTopicCreation: true,
	createPartitioner: kafkajs.Partitioners.DefaultPartitioner,
	maxConnectionRetries: 5,
	retryDelayMs: 1e3,
	compression: kafkajs.CompressionTypes.GZIP,
	metricsEnabled: true,
	logLevel: kafkajs.logLevel.INFO,
	rebalanceTimeout: 6e4,
	sessionTimeout: 3e4,
	heartbeatInterval: 3e3,
	requestTimeout: 3e4
};
var KafkaClient = class {
	producer;
	kafka;
	options;
	isConnected = false;
	connectionRetries = 0;
	metrics = {
		publishSuccess: 0,
		publishFailures: 0,
		connectionRetries: 0,
		consumerConnections: 0
	};
	admin;
	consumers = /* @__PURE__ */ new Map();
	constructor(clientId, brokers, options = {}) {
		this.options = {
			...DEFAULT_OPTIONS,
			...options
		};
		this.kafka = new kafkajs.Kafka({
			clientId,
			brokers,
			logLevel: this.options.logLevel,
			requestTimeout: this.options.requestTimeout
		});
		this.producer = this.kafka.producer({
			allowAutoTopicCreation: this.options.allowAutoTopicCreation,
			createPartitioner: this.options.createPartitioner,
			retry: {
				maxRetryTime: this.options.retryDelayMs * this.options.maxConnectionRetries,
				retries: this.options.maxConnectionRetries
			}
		});
		this.setupEventListeners();
	}
	setupEventListeners() {
		this.producer.on("producer.connect", () => {
			this.isConnected = true;
			require_logger.logger.info("Kafka producer connected");
		});
		this.producer.on("producer.disconnect", () => {
			this.isConnected = false;
			require_logger.logger.info("Kafka producer disconnected");
		});
		this.producer.on("producer.network.request_timeout", (payload) => {
			require_logger.logger.error("Kafka producer network request timeout", payload);
		});
	}
	async connect() {
		if (this.isConnected) return;
		while (this.connectionRetries < this.options.maxConnectionRetries) try {
			await this.producer.connect();
			return;
		} catch (error) {
			this.connectionRetries++;
			this.metrics.connectionRetries++;
			if (this.connectionRetries >= this.options.maxConnectionRetries) {
				require_logger.logger.error("Max connection retries reached", error);
				throw new KafkaError("Failed to connect to Kafka", error, { retries: this.connectionRetries });
			}
			const delay = Math.min(this.options.retryDelayMs * 2 ** this.connectionRetries, 3e4);
			await new Promise((resolve) => setTimeout(resolve, delay));
		}
	}
	connectAsync() {
		this.connect().catch((err) => {
			require_logger.logger.error("Kafka async connect failed", err);
		});
	}
	async disconnect() {
		try {
			for (const [groupId, consumer] of this.consumers.entries()) try {
				require_logger.logger.info("Disconnecting consumer during client shutdown", { groupId });
				await consumer.disconnect();
			} catch (err) {
				require_logger.logger.warn("Error while disconnecting consumer", {
					groupId,
					error: err
				});
			}
			this.consumers.clear();
			await this.producer.disconnect();
			this.isConnected = false;
			if (this.admin) await this.admin.disconnect();
		} catch (error) {
			require_logger.logger.error("Failed to disconnect Kafka client", error);
			throw new KafkaError("Failed to disconnect from Kafka", error);
		}
	}
	async runTransaction(callback) {
		if (!this.isConnected) throw new KafkaError("Producer not connected");
		const transaction = await this.producer.transaction();
		try {
			const result = await callback(transaction);
			await transaction.commit();
			return result;
		} catch (error) {
			await transaction.abort();
			throw new KafkaError("Kafka transaction failed", error);
		}
	}
	async sendMessages(topic, messages, compression = this.options.compression) {
		if (!this.isConnected) throw new KafkaError("Producer not connected");
		try {
			await this.producer.send({
				topic,
				messages: messages.map((m) => ({
					key: m.key,
					value: JSON.stringify(m.value),
					headers: m.headers,
					timestamp: m.timestamp,
					partition: m.partition
				})),
				compression
			});
			if (this.options.metricsEnabled) this.metrics.publishSuccess += messages.length;
			require_logger.logger.debug(`Successfully sent ${messages.length} messages to topic: ${topic}`);
		} catch (error) {
			if (this.options.metricsEnabled) this.metrics.publishFailures += messages.length;
			require_logger.logger.error(`Failed to send messages to topic "${topic}"`, error);
			throw new KafkaError("Messages send failed", error, { topic });
		}
	}
	async sendMessage(topic, message, compression = this.options.compression) {
		return this.sendMessages(topic, [message], compression);
	}
	getAdmin() {
		if (!this.admin) this.admin = this.kafka.admin();
		return this.admin;
	}
	async createConsumer(groupId, options) {
		const { groupId: _maybe, retry: _retry, sessionTimeout, heartbeatInterval, rebalanceTimeout, groupInstanceId, ...restOptions } = options;
		const consumerOptions = {
			groupId,
			...restOptions,
			sessionTimeout: sessionTimeout ?? this.options.sessionTimeout,
			heartbeatInterval: heartbeatInterval ?? this.options.heartbeatInterval,
			rebalanceTimeout: rebalanceTimeout ?? this.options.rebalanceTimeout,
			groupInstanceId: groupInstanceId ?? void 0,
			retry: {
				maxRetryTime: 3e4,
				initialRetryTime: 1e3,
				factor: 2,
				multiplier: 2,
				retries: 5,
				...options.retry || {}
			}
		};
		const consumer = this.kafka.consumer(consumerOptions);
		this.consumers.set(groupId, consumer);
		this.metrics.consumerConnections++;
		consumer.on("consumer.connect", () => {
			require_logger.logger.info("Kafka consumer connected", { groupId });
		});
		consumer.on("consumer.disconnect", () => {
			require_logger.logger.info("Kafka consumer disconnected", { groupId });
			this.metrics.consumerConnections--;
		});
		consumer.on("consumer.network.request_timeout", (payload) => {
			require_logger.logger.warn("Kafka consumer network request timeout", {
				groupId,
				payload
			});
		});
		consumer.on("consumer.crash", (err) => {
			require_logger.logger.error("Kafka consumer crash", {
				groupId,
				error: err
			});
			const tracked = this.consumers.get(groupId);
			if (!tracked) return;
			let retries = 0;
			Math.max(5, this.options.maxConnectionRetries);
			const attempt = async () => {
				if (!this.consumers.has(groupId)) return;
				try {
					require_logger.logger.info("Attempting to reconnect Kafka consumer", {
						groupId,
						attempt: retries + 1
					});
					await tracked.connect();
					require_logger.logger.info("Kafka consumer reconnected", { groupId });
				} catch (e) {
					retries++;
					this.metrics.connectionRetries++;
					const delay = Math.min(1e3 * 2 ** retries, 3e4);
					require_logger.logger.warn("Kafka consumer reconnect failed, scheduling retry", {
						groupId,
						retries,
						delay,
						error: e
					});
					setTimeout(attempt, delay);
				}
			};
			setTimeout(attempt, 1e3);
		});
		consumer.on("consumer.group_join", (info) => {
			require_logger.logger.info("Consumer group join", {
				groupId,
				info
			});
		});
		return consumer;
	}
	createProducer(overrides) {
		const opts = {
			allowAutoTopicCreation: this.options.allowAutoTopicCreation,
			createPartitioner: this.options.createPartitioner,
			retry: {
				maxRetryTime: this.options.retryDelayMs * this.options.maxConnectionRetries,
				retries: this.options.maxConnectionRetries
			},
			...overrides || {}
		};
		return this.kafka.producer(opts);
	}
	getProducer() {
		return this.producer;
	}
	getMetrics() {
		return { ...this.metrics };
	}
	isReady() {
		return this.isConnected;
	}
	async healthCheck() {
		try {
			if (!this.isConnected) await this.connect();
			const admin = this.getAdmin();
			await admin.connect();
			const clusterInfo = await admin.describeCluster();
			await admin.disconnect();
			return !!clusterInfo.controller;
		} catch (error) {
			require_logger.logger.error("Kafka health check failed", error);
			return false;
		}
	}
};

//#endregion
//#region src/client/base.ts
var BaseProducer = class {
	producer;
	serializer;
	metrics;
	constructor(producer, metrics, serializer = new require_types.JsonSerializer()) {
		this.producer = producer;
		this.serializer = serializer;
		this.metrics = metrics || {
			publishSuccess: 0,
			publishFailures: 0,
			connectionRetries: 0,
			consumerConnections: 0
		};
	}
	/**
	* Publishes a single message to the configured Kafka topic.
	* It handles serialization and logs the outcome.
	* @param data The Kafka message object to publish.
	* @returns A promise that resolves when the message has been successfully published.
	* @throws {KafkaError} if the publish operation fails.
	*/
	async publish(data) {
		try {
			require_logger.logger.info(`Publishing to ${this.topic}`, { key: data.key });
			await this.producer.send({
				topic: this.topic,
				messages: [{
					key: data.key,
					value: this.serializer.serialize(data.value),
					headers: data.headers,
					timestamp: data.timestamp
				}]
			});
			this.metrics.publishSuccess++;
			require_logger.logger.debug(`Message published to ${this.topic}`);
		} catch (error) {
			this.metrics.publishFailures++;
			require_logger.logger.error(`Publish failed to ${this.topic}`, error);
			throw new KafkaError(`Failed to publish to ${this.topic}`, error, { key: data.key });
		}
	}
	/**
	* Publishes a batch of messages to the configured Kafka topic.
	* This is more efficient for sending multiple messages at once.
	* @param messages An array of Kafka messages to publish.
	* @returns A promise that resolves when the batch has been successfully published.
	* @throws {KafkaError} if the batch publish operation fails.
	*/
	async publishBatch(messages) {
		try {
			await this.producer.send({
				topic: this.topic,
				messages: messages.map((msg) => ({
					key: msg.key,
					value: this.serializer.serialize(msg.value),
					headers: msg.headers,
					timestamp: msg.timestamp
				}))
			});
			this.metrics.publishSuccess += messages.length;
		} catch (error) {
			this.metrics.publishFailures += messages.length;
			throw new KafkaError(`Batch publish failed to ${this.topic}`, error, { messageCount: messages.length });
		}
	}
};

//#endregion
//#region src/utils/helpers.ts
/**
* Generates a correlation ID with optional prefix
* @param prefix - Optional prefix for human readability
* @returns Formatted correlation ID (e.g., "reg_AbCdEf123")
*/
const generateCorrelationId = (prefix) => {
	const id = (0, nanoid.nanoid)(12);
	return prefix ? `${prefix}_${id}` : id;
};

//#endregion
//#region src/client/event.factory.ts
var GenericKafkaProducer = class extends BaseProducer {
	topic;
	keyDeriver;
	constructor(producer, topic, keyDeriver) {
		super(producer);
		this.topic = topic;
		this.keyDeriver = keyDeriver;
	}
	generateKey(payload) {
		return this.keyDeriver(payload);
	}
	enrichAndFormatMessage(payload) {
		const key = this.generateKey(payload);
		const rawTimestamp = payload.timestamp;
		const timestamp = rawTimestamp instanceof Date ? rawTimestamp : new Date(String(rawTimestamp));
		const metadata = payload.metadata ?? {};
		const correlationId = metadata.correlationId;
		const schemaHash = metadata.schemaHash;
		const source = metadata.source;
		return {
			key,
			headers: {
				"event-version": "1.0",
				"event-type": this.topic,
				timestamp: timestamp.toISOString(),
				...correlationId ? { "correlation-id": String(correlationId) } : {},
				...source ? { source: String(source) } : {},
				...schemaHash ? { "schema-hash": String(schemaHash) } : {}
			},
			value: payload
		};
	}
	stringifyHeaders(headers) {
		if (!headers) return void 0;
		const result = {};
		for (const [k, v] of Object.entries(headers)) result[k] = v instanceof Date ? v.toISOString() : String(v);
		return result;
	}
	async send(payload) {
		const enriched = this.enrichAndFormatMessage(payload);
		const stringifiedHeaders = this.stringifyHeaders(enriched.headers);
		await this.publish({
			key: enriched.key,
			value: enriched.value,
			headers: stringifiedHeaders
		});
		require_logger.logger.info(`Published event to topic ${this.topic}`, {
			key: enriched.key,
			timestamp: enriched.value.timestamp?.toString()
		});
	}
};
/**
* Factory for creating validated event publishers.
*/
function createEventPublisher(producer, topicName, schema, keyDeriver, source) {
	const producerInstance = new GenericKafkaProducer(producer, topicName, keyDeriver);
	return async (payload) => {
		const validated = schema.parse(payload);
		const rawMeta = validated?.metadata;
		const safeMeta = rawMeta && typeof rawMeta === "object" && !Array.isArray(rawMeta) ? rawMeta : {};
		const event = {
			...validated,
			metadata: {
				source,
				correlationId: generateCorrelationId("evt"),
				...safeMeta
			}
		};
		await producerInstance.send(event);
	};
}

//#endregion
//#region src/client/connect-with-retry.ts
/**
* Generic helper to retry an async connect function with exponential backoff.
* Detects transient DNS errors (EAI_AGAIN) and will keep retrying up to maxAttempts.
*/
async function connectWithRetry(connectFn, opts = {}) {
	const { maxAttempts = 8, initialDelayMs = 1e3, maxDelayMs = 3e4, factor = 2 } = opts;
	let attempt = 0;
	while (true) try {
		attempt++;
		await connectFn();
		if (attempt > 1) require_logger.logger.info("connectWithRetry: connection succeeded", { attempt });
		return;
	} catch (err) {
		const isDnsError = err?.code === "EAI_AGAIN" || typeof err?.message === "string" && err.message.includes("EAI_AGAIN");
		if (!(isDnsError || attempt < maxAttempts)) {
			require_logger.logger.error("connectWithRetry: no more retries, failing", {
				attempt,
				error: err
			});
			throw err;
		}
		const delay = Math.min(initialDelayMs * factor ** attempt, maxDelayMs);
		require_logger.logger.warn("connectWithRetry: connect failed, retrying", {
			attempt,
			delay,
			retriable: isDnsError,
			error: err?.message || err
		});
		await new Promise((res) => setTimeout(res, delay));
	}
}

//#endregion
Object.defineProperty(exports, 'BaseProducer', {
  enumerable: true,
  get: function () {
    return BaseProducer;
  }
});
Object.defineProperty(exports, 'KafkaClient', {
  enumerable: true,
  get: function () {
    return KafkaClient;
  }
});
Object.defineProperty(exports, 'KafkaError', {
  enumerable: true,
  get: function () {
    return KafkaError;
  }
});
Object.defineProperty(exports, 'connectWithRetry', {
  enumerable: true,
  get: function () {
    return connectWithRetry;
  }
});
Object.defineProperty(exports, 'createEventPublisher', {
  enumerable: true,
  get: function () {
    return createEventPublisher;
  }
});