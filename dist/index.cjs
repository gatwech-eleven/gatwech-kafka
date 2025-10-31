var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  BaseProducer: () => BaseProducer,
  BaseProducerSchema: () => BaseProducerSchema,
  JsonSerializer: () => JsonSerializer,
  KafkaClient: () => KafkaClient,
  KafkaError: () => KafkaError,
  chatTopics: () => chatTopics,
  connectWithRetry: () => connectWithRetry,
  createEventPublisher: () => createEventPublisher,
  ensureTopicsWithConfig: () => ensureTopicsWithConfig,
  notificationTopics: () => notificationTopics,
  orderTopics: () => orderTopics,
  paymentTopics: () => paymentTopics,
  producer: () => validate_and_send_exports,
  resolveBrokersFromEnv: () => resolveBrokersFromEnv,
  schemas: () => schemas_exports,
  topics: () => topics,
  userTopics: () => userTopics,
  validation: () => validation_exports
});
module.exports = __toCommonJS(index_exports);

// src/utils/logger.ts
var import_winston = require("winston");
var { combine, timestamp, printf, colorize } = import_winston.format;
var defaultFormat = printf(
  ({ level, message, timestamp: timestamp2, service, ...meta }) => {
    const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
    return `${timestamp2} [${service || "kafka"}] ${level}: ${message}${metaStr}`;
  }
);
function createServiceLogger(serviceName = "kafka") {
  return (0, import_winston.createLogger)({
    level: process.env.LOG_LEVEL || (process.env.NODE_ENV === "production" ? "info" : "debug"),
    format: combine(
      timestamp(),
      colorize({ all: true }),
      (0, import_winston.format)((info) => ({ ...info, service: serviceName }))(),
      defaultFormat
    ),
    transports: [new import_winston.transports.Console()]
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
var import_kafkajs = require("kafkajs");
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
  createPartitioner: import_kafkajs.Partitioners.DefaultPartitioner,
  maxConnectionRetries: 5,
  retryDelayMs: 1e3,
  compression: import_kafkajs.CompressionTypes.GZIP,
  metricsEnabled: true,
  logLevel: import_kafkajs.logLevel.INFO,
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
    this.kafka = new import_kafkajs.Kafka({
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
var import_nanoid = require("nanoid");
var generateCorrelationId = (prefix) => {
  const id = (0, import_nanoid.nanoid)(12);
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

// src/config/topics/topic-config-schema.ts
var import_zod = require("@payvo/utils/zod");
var TopicConfigSchema = import_zod.z.object({
  name: import_zod.z.string(),
  retentionMs: import_zod.z.number().default(6048e5),
  cleanupPolicy: import_zod.z.enum(["delete", "compact", "compact,delete"]).default("delete")
});

// src/config/topics/user-topics.ts
var userTopics = {
  // User Auth Topics
  USER_REGISTERED: TopicConfigSchema.parse({
    name: "user.registered"
  }),
  USER_UPDATED: TopicConfigSchema.parse({
    name: "user.updated"
  }),
  USER_LOGGED_IN: TopicConfigSchema.parse({
    name: "user.logged.in"
  }),
  USER_LOGGED_OUT: {
    name: "user.logged.out",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  },
  USER_PASSWORD_RESET: TopicConfigSchema.parse({
    name: "user.password.reset"
  }),
  USER_RESET_PASSWORD_REQUEST: TopicConfigSchema.parse({
    name: "user.reset.password.request",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  }),
  USER_VERIFIED: TopicConfigSchema.parse({
    name: "user.verified",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  }),
  // User KYC Topics
  USER_KYC_APPLICATION_CREATED: TopicConfigSchema.parse({
    name: "user.kyc.application.created"
  }),
  USER_KYC_APPLICATION_UPDATED: TopicConfigSchema.parse({
    name: "user.kyc.application.updated"
  }),
  USER_KYC_DOCUMENTS_UPLOADED: TopicConfigSchema.parse({
    name: "user.kyc.documents.uploaded"
  }),
  USER_KYC_DOCUMENTS_REVIEWED: TopicConfigSchema.parse({
    name: "user.kyc.documents.reviewed"
  }),
  USER_KYC_DOCUMENTS_APPROVED: TopicConfigSchema.parse({
    name: "user.kyc.documents.approved"
  }),
  USER_KYC_DOCUMENTS_REJECTED: TopicConfigSchema.parse({
    name: "user.kyc.documents.rejected"
  }),
  USER_KYC_APPLICATION_COMPLETED: TopicConfigSchema.parse({
    name: "user.kyc.application.completed"
  }),
  // Phone Number Topics
  USER_PHONE_NUMBER_ADDED: TopicConfigSchema.parse({
    name: "user.phone.number.added"
  }),
  USER_PHONE_NUMBER_REMOVED: TopicConfigSchema.parse({
    name: "user.phone.number.removed"
  }),
  USER_PHONE_NUMBER_VERIFIED: TopicConfigSchema.parse({
    name: "user.phone.number.verified"
  }),
  // User Suspension Topics
  USER_SUSPENDED: TopicConfigSchema.parse({ name: "user.suspended" }),
  USER_UNSUSPENDED: TopicConfigSchema.parse({ name: "user.unsuspended" })
};

// src/config/topics/payment-topics.ts
var paymentTopics = {
  PAYMENT_CREATED: TopicConfigSchema.parse({ name: "payment.created" }),
  PAYMENT_COMPLETED: TopicConfigSchema.parse({ name: "payment.completed" }),
  PAYMENT_FAILED: TopicConfigSchema.parse({ name: "payment.failed" })
};

// src/config/topics/notification-topics.ts
var notificationTopics = {
  EMAIL_SENT: TopicConfigSchema.parse({ name: "notification.email.sent" }),
  SMS_SENT: TopicConfigSchema.parse({ name: "notification.sms.sent" })
};

// src/config/topics/chat-topic.ts
var chatTopics = {
  CHAT_MESSAGE_SENT: TopicConfigSchema.parse({
    name: "notification.chat.message.sent"
  })
};

// src/config/topics/order-topics.ts
var orderTopics = {
  ORDER_CREATED: TopicConfigSchema.parse({ name: "order.created" }),
  ORDER_UPDATED: TopicConfigSchema.parse({ name: "order.updated" }),
  ORDER_CANCELED: TopicConfigSchema.parse({ name: "order.canceled" }),
  ORDER_COMPLETED: TopicConfigSchema.parse({ name: "order.completed" }),
  ORDER_DISPUTED: TopicConfigSchema.parse({ name: "order.disputed" })
};

// src/config/topics/index.ts
var topics = {
  user: userTopics,
  payment: paymentTopics,
  notification: notificationTopics
};

// src/config/ensure-topics.ts
var CONFIG_RESOURCE_TYPE_TOPIC = 2;
var asTopicDefs = (topics2) => Object.values(topics2).map((t) => ({
  name: t.name,
  config: t.config
}));
var toConfigEntries = (cfg) => [
  { name: "retention.ms", value: String(cfg.retentionMs) },
  { name: "cleanup.policy", value: cfg.cleanupPolicy }
];
function validateTopics(defs) {
  const names = /* @__PURE__ */ new Set();
  for (const t of defs) {
    if (!t.name) throw new Error("TOPICS entry missing name");
    if (names.has(t.name)) throw new Error(`Duplicate topic name: ${t.name}`);
    names.add(t.name);
    if (!["delete", "compact", "compact,delete"].includes(t.config.cleanupPolicy)) {
      logger.warn(
        "unexpected cleanup.policy value; expected delete|compact|compact,delete",
        {
          topic: t.name,
          cleanupPolicy: t.config.cleanupPolicy
        }
      );
    }
    if (t.config.cleanupPolicy === "compact" && t.config.retentionMs > 0) {
      logger.warn(
        'retention.ms is set but cleanup.policy is "compact"; retention is ignored unless policy is "compact,delete"',
        { topic: t.name, retentionMs: t.config.retentionMs }
      );
    }
  }
}
async function waitForLeaders(admin, topic, expectedPartitions, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  if (expectedPartitions <= 0) return;
  while (true) {
    const md = await admin.fetchTopicMetadata({ topics: [topic] });
    const t = md.topics.find((x) => x.name === topic);
    const leadersOk = !!t && t.partitions.length >= expectedPartitions && t.partitions.every((p) => p.leader !== -1);
    if (leadersOk) return;
    if (Date.now() >= deadline) {
      logger.warn("waitForLeaders timeout; leaders may still be electing", {
        topic,
        have: t?.partitions.length,
        expected: expectedPartitions
      });
      return;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}
async function safeDescribeConfigs(admin, topics2) {
  try {
    const resp = await admin.describeConfigs?.(
      {
        resources: topics2.map((name) => ({
          type: CONFIG_RESOURCE_TYPE_TOPIC,
          name
        })),
        includeSynonyms: false,
        includeDocumentation: false
      }
    );
    return resp;
  } catch {
    return void 0;
  }
}
async function ensureTopicsWithConfig(admin, opts = {}, manageConnection = true) {
  const partitionsDefault = opts.partitions ?? 3;
  const rfDefault = opts.replicationFactor ?? 1;
  const waitForLeadersMs = opts.waitForLeadersMs ?? 15e3;
  const flatTopics = Object.fromEntries(
    Object.values(topics).flatMap(
      (group) => Object.values(group).map((t) => [
        t.name,
        { name: t.name, config: t }
      ])
    )
  );
  const defs = asTopicDefs(flatTopics);
  validateTopics(defs);
  if (manageConnection) await admin.connect();
  try {
    const md = await admin.fetchTopicMetadata();
    const existing = new Set(md.topics.map((t) => t.name));
    const createList = defs.filter((t) => !existing.has(t.name)).map((t) => ({
      topic: t.name,
      numPartitions: t.config.partitions ?? partitionsDefault,
      replicationFactor: t.config.replicationFactor ?? rfDefault,
      configEntries: toConfigEntries(t.config)
    }));
    if (createList.length) {
      logger.info("creating Kafka topics", {
        topics: createList.map((t) => t.topic)
      });
      try {
        await admin.createTopics({ topics: createList, waitForLeaders: true });
      } catch (e) {
        if (!/already exists/i.test(e?.message || "")) throw e;
      }
      for (const t of createList) {
        await waitForLeaders(
          admin,
          t.topic,
          t.numPartitions ?? partitionsDefault,
          waitForLeadersMs
        );
      }
    }
    const md2 = await admin.fetchTopicMetadata();
    const current = new Map(md2.topics.map((t) => [t.name, t]));
    const partitionsToIncrease = defs.map((t) => {
      const desired = t.config.partitions ?? partitionsDefault;
      const curCount = current.get(t.name)?.partitions?.length ?? 0;
      return desired > curCount ? { topic: t.name, count: desired } : null;
    }).filter(Boolean);
    if (partitionsToIncrease.length) {
      logger.info("increasing partitions", partitionsToIncrease);
      try {
        await admin.createPartitions({ topicPartitions: partitionsToIncrease });
      } catch (e) {
        logger.warn("createPartitions returned error; continuing", {
          error: e?.message
        });
      }
      for (const t of partitionsToIncrease) {
        await waitForLeaders(admin, t.topic, t.count, waitForLeadersMs);
      }
    }
    const topics2 = defs.map((t) => t.name);
    const desiredByTopic = new Map(
      defs.map((t) => [t.name, toConfigEntries(t.config)])
    );
    const desc = await safeDescribeConfigs(admin, topics2);
    let resources;
    if (desc?.resources?.length) {
      const currentByTopic = new Map(
        desc.resources.map((r) => [
          r.resourceName,
          new Map((r.configs || []).map((c) => [c.name, c.value]))
        ])
      );
      const pending = [];
      for (const name of topics2) {
        const desired = desiredByTopic.get(name);
        const currentCfg = currentByTopic.get(name);
        const diff = desired.filter(
          (entry) => currentCfg?.get(entry.name) !== entry.value
        );
        if (diff.length)
          pending.push({
            type: CONFIG_RESOURCE_TYPE_TOPIC,
            name,
            configEntries: diff
          });
      }
      resources = pending;
    } else {
      resources = topics2.map((name) => ({
        type: CONFIG_RESOURCE_TYPE_TOPIC,
        name,
        configEntries: desiredByTopic.get(name)
      }));
    }
    if (resources.length) {
      try {
        await admin.alterConfigs({ resources, validateOnly: false });
        logger.info("topic configs ensured", {
          topics: resources.map((r) => r.name)
        });
      } catch (e) {
        logger.warn(
          "alterConfigs failed (check ACLs / broker version). Topics still created.",
          {
            error: e?.message
          }
        );
      }
    } else {
      logger.debug?.("topic configs already up-to-date");
    }
  } finally {
    if (manageConnection) await admin.disconnect();
  }
}

// src/validation/index.ts
var validation_exports = {};
__export(validation_exports, {
  default: () => validation_default,
  validateMessage: () => validateMessage
});
var import_utils = require("@payvo/utils");
function validateMessage(schema, value, context) {
  const res = schema.safeParse(value);
  if (res.success) return { success: true, data: res.data };
  const tree = import_utils.z.treeifyError(res.error);
  logger.warn("Kafka message validation failed", {
    ...context || {},
    errors: tree
  });
  return { success: false, error: tree };
}
var validation_default = { validateMessage };

// src/producer/validate-and-send.ts
var validate_and_send_exports = {};
__export(validate_and_send_exports, {
  default: () => validate_and_send_default,
  validateAndSend: () => validateAndSend,
  validateAndSendMany: () => validateAndSendMany
});
var import_utils2 = require("@payvo/utils");
async function validateAndSend(client, topic, schema, message) {
  const res = schema.safeParse(message.value);
  if (!res.success) {
    const tree = import_utils2.z.treeifyError(res.error);
    logger.warn("Producer validation failed; not sending message", {
      topic,
      errors: tree
    });
    throw new KafkaError("Validation failed for message", res.error);
  }
  return client.sendMessage(topic, message);
}
async function validateAndSendMany(client, topic, schema, messages) {
  for (const msg of messages) {
    const res = schema.safeParse(msg.value);
    if (!res.success) {
      const tree = import_utils2.z.treeifyError(res.error);
      logger.warn("Producer batch validation failed; not sending batch", {
        topic,
        errors: tree
      });
      throw new KafkaError("Validation failed for batch message", res.error);
    }
  }
  return client.sendMessages(topic, messages);
}
var validate_and_send_default = { validateAndSend, validateAndSendMany };

// src/schemas/index.ts
var schemas_exports = {};
__export(schemas_exports, {
  NotificationEmailSentSchema: () => NotificationEmailSentSchema,
  ResetPasswordRequestEmailSchema: () => ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema: () => ResetPasswordRequestPhoneSchema,
  ResetPasswordRequestUnionSchema: () => ResetPasswordRequestUnionSchema,
  UserRegisteredSchema: () => UserRegisteredSchema,
  notificationSchemas: () => notification_schemas_default,
  userSchemas: () => user_schemas_default
});

// src/schemas/user-schemas.ts
var import_zod2 = require("@payvo/utils/zod");
var UserRegisteredSchema = import_zod2.z.object({
  id: import_zod2.z.string(),
  email: import_zod2.z.email(),
  name: import_zod2.z.string().optional(),
  otp: import_zod2.z.string().optional(),
  timestamp: import_zod2.z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: import_zod2.z.object({
    correlationId: import_zod2.z.string().optional(),
    source: import_zod2.z.string().optional()
  }).optional()
});
var ResetPasswordRequestEmailSchema = import_zod2.z.object({
  userId: import_zod2.z.uuid().optional(),
  identifier: import_zod2.z.literal("email"),
  email: import_zod2.z.email(),
  token: import_zod2.z.string(),
  timestamp: import_zod2.z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: import_zod2.z.object({
    correlationId: import_zod2.z.string().optional(),
    source: import_zod2.z.string().optional()
  }).optional()
});
var ResetPasswordRequestPhoneSchema = import_zod2.z.object({
  userId: import_zod2.z.uuid().optional(),
  identifier: import_zod2.z.literal("phone"),
  phoneNumber: import_zod2.z.string(),
  otp: import_zod2.z.string().optional(),
  token: import_zod2.z.string().optional(),
  timestamp: import_zod2.z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: import_zod2.z.object({
    correlationId: import_zod2.z.string().optional(),
    source: import_zod2.z.string().optional()
  }).optional()
});
var ResetPasswordRequestUnionSchema = import_zod2.z.union([
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema
]);
var user_schemas_default = {
  UserRegisteredSchema,
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema,
  ResetPasswordRequestUnionSchema
};

// src/schemas/notification-schemas.ts
var import_zod3 = require("@payvo/utils/zod");
var NotificationEmailSentSchema = import_zod3.z.object({
  to: import_zod3.z.string().email(),
  subject: import_zod3.z.string(),
  body: import_zod3.z.string(),
  timestamp: import_zod3.z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: import_zod3.z.object({
    correlationId: import_zod3.z.string().optional(),
    source: import_zod3.z.string().optional()
  }).optional()
});
var notification_schemas_default = { NotificationEmailSentSchema };

// src/utils/brokers.ts
function resolveBrokersFromEnv() {
  const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
  if (raw && raw.trim().length > 0) {
    return raw.split(",").map((s) => s.trim()).filter(Boolean);
  }
  if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") {
    return ["kafka:9092"];
  }
  return ["localhost:9092"];
}

// src/schemas/base.schema.ts
var import_zod4 = require("@payvo/utils/zod");
var BaseProducerSchema = import_zod4.z.object({
  timestamp: import_zod4.z.date().optional().default(() => /* @__PURE__ */ new Date()),
  metadata: import_zod4.z.any().optional()
});
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  BaseProducer,
  BaseProducerSchema,
  JsonSerializer,
  KafkaClient,
  KafkaError,
  chatTopics,
  connectWithRetry,
  createEventPublisher,
  ensureTopicsWithConfig,
  notificationTopics,
  orderTopics,
  paymentTopics,
  producer,
  resolveBrokersFromEnv,
  schemas,
  topics,
  userTopics,
  validation
});
