import { l as logger } from './kafka.Dv3r60xd.mjs';
import { logLevel, CompressionTypes, Partitioners, Kafka } from 'kafkajs';

class KafkaError extends Error {
  constructor(message, cause, meta) {
    super(message);
    this.cause = cause;
    this.meta = meta;
    this.name = "KafkaError";
    Object.setPrototypeOf(this, KafkaError.prototype);
  }
}
const DEFAULT_OPTIONS = {
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
class KafkaClient {
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
      Math.max(5, this.options.maxConnectionRetries);
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
}

export { KafkaError as K, KafkaClient as a };
