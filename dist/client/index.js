// src/utils/logger.ts
import { createLogger, format, transports } from "winston";
var { combine, timestamp, printf, colorize } = format;
var defaultFormat = printf(
  ({ level, message, timestamp: timestamp2, service, ...meta }) => {
    const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
    return `${timestamp2} [${service || "kafka"}] ${level}: ${message}${metaStr}`;
  }
);
function createServiceLogger(serviceName = "kafka") {
  return createLogger({
    level: process.env.LOG_LEVEL || (process.env.NODE_ENV === "production" ? "info" : "debug"),
    format: combine(
      timestamp(),
      colorize({ all: true }),
      format((info) => ({ ...info, service: serviceName }))(),
      defaultFormat
    ),
    transports: [new transports.Console()]
  });
}
var loggerInstance = createServiceLogger(process.env.SERVICE_NAME || "kafka");
var logger = loggerInstance;

// src/types/kafka.ts
var JsonSerializer = class {
  serialize(value) {
    return JSON.stringify(value);
  }
  deserialize(value) {
    return JSON.parse(value);
  }
};

// src/client/kafka-client.ts
import { Kafka, Partitioners, CompressionTypes, logLevel } from "kafkajs";
var KafkaError = class _KafkaError extends Error {
  constructor(message, cause, meta) {
    super(message);
    this.cause = cause;
    this.meta = meta;
    this.name = "KafkaError";
    Object.setPrototypeOf(this, _KafkaError.prototype);
  }
};
var DEFAULT_OPTIONS = {
  allowAutoTopicCreation: true,
  createPartitioner: Partitioners.DefaultPartitioner,
  maxConnectionRetries: 5,
  retryDelayMs: 1e3,
  compression: CompressionTypes.GZIP,
  metricsEnabled: true,
  logLevel: logLevel.INFO,
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
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.kafka = new Kafka({
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
      logger.info("Kafka producer connected");
    });
    this.producer.on("producer.disconnect", () => {
      this.isConnected = false;
      logger.info("Kafka producer disconnected");
    });
    this.producer.on("producer.network.request_timeout", (payload) => {
      logger.error("Kafka producer network request timeout", payload);
    });
  }
  async connect() {
    if (this.isConnected) return;
    while (this.connectionRetries < this.options.maxConnectionRetries) {
      try {
        await this.producer.connect();
        return;
      } catch (error) {
        this.connectionRetries++;
        this.metrics.connectionRetries++;
        if (this.connectionRetries >= this.options.maxConnectionRetries) {
          logger.error("Max connection retries reached", error);
          throw new KafkaError("Failed to connect to Kafka", error, {
            retries: this.connectionRetries
          });
        }
        const delay = Math.min(
          this.options.retryDelayMs * 2 ** this.connectionRetries,
          3e4
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  connectAsync() {
    void this.connect().catch((err) => {
      logger.error("Kafka async connect failed", err);
    });
  }
  async disconnect() {
    try {
      for (const [groupId, consumer] of this.consumers.entries()) {
        try {
          logger.info("Disconnecting consumer during client shutdown", {
            groupId
          });
          await consumer.disconnect();
        } catch (err) {
          logger.warn("Error while disconnecting consumer", {
            groupId,
            error: err
          });
        }
      }
      this.consumers.clear();
      await this.producer.disconnect();
      this.isConnected = false;
      if (this.admin) {
        await this.admin.disconnect();
      }
    } catch (error) {
      logger.error("Failed to disconnect Kafka client", error);
      throw new KafkaError("Failed to disconnect from Kafka", error);
    }
  }
  async runTransaction(callback) {
    if (!this.isConnected) {
      throw new KafkaError("Producer not connected");
    }
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
    if (!this.isConnected) {
      throw new KafkaError("Producer not connected");
    }
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
      if (this.options.metricsEnabled) {
        this.metrics.publishSuccess += messages.length;
      }
      logger.debug(
        `Successfully sent ${messages.length} messages to topic: ${topic}`
      );
    } catch (error) {
      if (this.options.metricsEnabled) {
        this.metrics.publishFailures += messages.length;
      }
      logger.error(`Failed to send messages to topic "${topic}"`, error);
      throw new KafkaError("Messages send failed", error, { topic });
    }
  }
  async sendMessage(topic, message, compression = this.options.compression) {
    return this.sendMessages(topic, [message], compression);
  }
  getAdmin() {
    if (!this.admin) {
      this.admin = this.kafka.admin();
    }
    return this.admin;
  }
  async createConsumer(groupId, options) {
    const {
      groupId: _maybe,
      retry: _retry,
      sessionTimeout,
      heartbeatInterval,
      rebalanceTimeout,
      groupInstanceId,
      ...restOptions
    } = options;
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
      logger.info("Kafka consumer connected", { groupId });
    });
    consumer.on("consumer.disconnect", () => {
      logger.info("Kafka consumer disconnected", { groupId });
      this.metrics.consumerConnections--;
    });
    consumer.on("consumer.network.request_timeout", (payload) => {
      logger.warn("Kafka consumer network request timeout", {
        groupId,
        payload
      });
    });
    consumer.on("consumer.crash", (err) => {
      logger.error("Kafka consumer crash", { groupId, error: err });
      const tracked = this.consumers.get(groupId);
      if (!tracked) return;
      let retries = 0;
      const maxRetries = Math.max(5, this.options.maxConnectionRetries);
      const attempt = async () => {
        if (!this.consumers.has(groupId)) return;
        try {
          logger.info("Attempting to reconnect Kafka consumer", {
            groupId,
            attempt: retries + 1
          });
          await tracked.connect();
          logger.info("Kafka consumer reconnected", { groupId });
        } catch (e) {
          retries++;
          this.metrics.connectionRetries++;
          const delay = Math.min(1e3 * 2 ** retries, 3e4);
          logger.warn("Kafka consumer reconnect failed, scheduling retry", {
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
      logger.info("Consumer group join", { groupId, info });
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
    const p = this.kafka.producer(opts);
    return p;
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
      if (!this.isConnected) {
        await this.connect();
      }
      const admin = this.getAdmin();
      await admin.connect();
      const clusterInfo = await admin.describeCluster();
      await admin.disconnect();
      return !!clusterInfo.controller;
    } catch (error) {
      logger.error("Kafka health check failed", error);
      return false;
    }
  }
};

// src/client/base.ts
var BaseProducer = class {
  producer;
  serializer;
  metrics;
  constructor(producer, metrics, serializer = new JsonSerializer()) {
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
      logger.info(`Publishing to ${this.topic}`, { key: data.key });
      await this.producer.send({
        topic: this.topic,
        messages: [
          {
            key: data.key,
            value: this.serializer.serialize(data.value),
            headers: data.headers,
            timestamp: data.timestamp
          }
        ]
      });
      this.metrics.publishSuccess++;
      logger.debug(`Message published to ${this.topic}`);
    } catch (error) {
      this.metrics.publishFailures++;
      logger.error(`Publish failed to ${this.topic}`, error);
      throw new KafkaError(
        `Failed to publish to ${this.topic}`,
        error,
        {
          key: data.key
        }
      );
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
      throw new KafkaError(
        `Batch publish failed to ${this.topic}`,
        error,
        {
          messageCount: messages.length
        }
      );
    }
  }
};

// src/utils/helpers.ts
import { nanoid } from "nanoid";
var generateCorrelationId = (prefix) => {
  const id = nanoid(12);
  return prefix ? `${prefix}_${id}` : id;
};

// src/client/event.factory.ts
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
    const timestamp2 = rawTimestamp instanceof Date ? rawTimestamp : new Date(String(rawTimestamp));
    const metadata = payload.metadata ?? {};
    const correlationId = metadata.correlationId;
    const schemaHash = metadata.schemaHash;
    const source = metadata.source;
    const headers = {
      "event-version": "1.0",
      "event-type": this.topic,
      timestamp: timestamp2.toISOString(),
      ...correlationId ? { "correlation-id": String(correlationId) } : {},
      ...source ? { source: String(source) } : {},
      ...schemaHash ? { "schema-hash": String(schemaHash) } : {}
    };
    return {
      key,
      headers,
      value: payload
    };
  }
  stringifyHeaders(headers) {
    if (!headers) return void 0;
    const result = {};
    for (const [k, v] of Object.entries(headers)) {
      result[k] = v instanceof Date ? v.toISOString() : String(v);
    }
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
    logger.info(`Published event to topic ${this.topic}`, {
      key: enriched.key,
      timestamp: enriched.value.timestamp?.toString()
    });
  }
};
function createEventPublisher(producer, topicName, schema, keyDeriver, source) {
  const producerInstance = new GenericKafkaProducer(
    producer,
    topicName,
    keyDeriver
  );
  return async (payload) => {
    const validated = schema.parse(payload);
    const event = {
      ...validated,
      metadata: {
        source,
        correlationId: generateCorrelationId("evt"),
        ...validated.metadata ?? {}
      }
    };
    await producerInstance.send(event);
  };
}

// src/client/connect-with-retry.ts
async function connectWithRetry(connectFn, opts = {}) {
  const {
    maxAttempts = 8,
    initialDelayMs = 1e3,
    maxDelayMs = 3e4,
    factor = 2
  } = opts;
  let attempt = 0;
  while (true) {
    try {
      attempt++;
      await connectFn();
      if (attempt > 1) {
        logger.info("connectWithRetry: connection succeeded", { attempt });
      }
      return;
    } catch (err) {
      const isDnsError = err?.code === "EAI_AGAIN" || typeof err?.message === "string" && err.message.includes("EAI_AGAIN");
      const willRetry = isDnsError || attempt < maxAttempts;
      if (!willRetry) {
        logger.error("connectWithRetry: no more retries, failing", {
          attempt,
          error: err
        });
        throw err;
      }
      const delay = Math.min(initialDelayMs * factor ** attempt, maxDelayMs);
      logger.warn("connectWithRetry: connect failed, retrying", {
        attempt,
        delay,
        retriable: isDnsError,
        error: err?.message || err
      });
      await new Promise((res) => setTimeout(res, delay));
    }
  }
}
export {
  BaseProducer,
  KafkaClient,
  KafkaError,
  connectWithRetry,
  createEventPublisher
};
