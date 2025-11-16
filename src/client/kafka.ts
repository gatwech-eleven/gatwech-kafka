import { logger } from "../utils/logger";
import { Kafka, Partitioners, CompressionTypes, logLevel } from "kafkajs";
import type {
  Producer,
  ConsumerConfig,
  Transaction,
  Admin,
  ProducerConfig,
} from "kafkajs";
import type { KafkaClientOptions, KafkaMetrics, TKafkaMessage } from "../types";
import type { StrictOmit } from "../types/utility-types";

export class KafkaError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
    public readonly meta?: Record<string, unknown>
  ) {
    super(message);
    this.name = "KafkaError";
    Object.setPrototypeOf(this, KafkaError.prototype);
  }
}

const DEFAULT_OPTIONS: Required<KafkaClientOptions> = {
  allowAutoTopicCreation: true,
  createPartitioner: Partitioners.DefaultPartitioner,
  maxConnectionRetries: 5,
  retryDelayMs: 1000,
  compression: CompressionTypes.GZIP,
  metricsEnabled: true,
  logLevel: logLevel.INFO,
  rebalanceTimeout: 60000,
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  requestTimeout: 30000,
};

export class KafkaClient {
  private readonly producer: Producer;
  private readonly kafka: Kafka;
  private readonly options: Required<KafkaClientOptions>;
  private isConnected = false;
  private connectionRetries = 0;
  private metrics: KafkaMetrics = {
    publishSuccess: 0,
    publishFailures: 0,
    connectionRetries: 0,
    consumerConnections: 0,
  };
  private admin?: Admin;
  private consumers: Map<string, ReturnType<Kafka["consumer"]>> = new Map();

  constructor(
    clientId: string,
    brokers: string[],
    options: KafkaClientOptions = {}
  ) {
    this.options = { ...DEFAULT_OPTIONS, ...options };

    this.kafka = new Kafka({
      clientId,
      brokers,
      logLevel: this.options.logLevel,
      requestTimeout: this.options.requestTimeout,
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: this.options.allowAutoTopicCreation,
      createPartitioner: this.options.createPartitioner,
      retry: {
        maxRetryTime:
          this.options.retryDelayMs * this.options.maxConnectionRetries,
        retries: this.options.maxConnectionRetries,
      },
    });

    this.setupEventListeners();
  }

  private setupEventListeners(): void {
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

  public async connect(): Promise<void> {
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
            retries: this.connectionRetries,
          });
        }

        const delay = Math.min(
          this.options.retryDelayMs * 2 ** this.connectionRetries,
          30000
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  public connectAsync(): void {
    void this.connect().catch((err) => {
      logger.error("Kafka async connect failed", err);
    });
  }

  public async disconnect(): Promise<void> {
    try {
      for (const [groupId, consumer] of this.consumers.entries()) {
        try {
          logger.info("Disconnecting consumer during client shutdown", {
            groupId,
          });
          await consumer.disconnect();
        } catch (err) {
          logger.warn("Error while disconnecting consumer", {
            groupId,
            error: err,
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

  public async runTransaction<T>(
    callback: (transaction: Transaction) => Promise<T>
  ): Promise<T> {
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

  public async sendMessages<T>(
    topic: string,
    messages: TKafkaMessage<T>[],
    compression: CompressionTypes = this.options.compression
  ): Promise<void> {
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
          partition: m.partition,
        })),
        compression,
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

  public async sendMessage<T>(
    topic: string,
    message: TKafkaMessage<T>,
    compression: CompressionTypes = this.options.compression
  ): Promise<void> {
    return this.sendMessages(topic, [message], compression);
  }

  public getAdmin(): Admin {
    if (!this.admin) {
      this.admin = this.kafka.admin();
    }
    return this.admin;
  }

  public async createConsumer(
    groupId: string,
    options: ConsumerConfig & { groupInstanceId?: string }
  ): Promise<ReturnType<Kafka["consumer"]>> {
    const {
      groupId: _maybe,
      retry: _retry,
      sessionTimeout,
      heartbeatInterval,
      rebalanceTimeout,
      groupInstanceId,
      ...restOptions
    } = options;

    type KafkaConsumerInit = ConsumerConfig & { groupInstanceId?: string };

    const consumerOptions: KafkaConsumerInit = {
      groupId,
      ...(restOptions as StrictOmit<ConsumerConfig, "groupId" | "retry">),
      sessionTimeout: sessionTimeout ?? this.options.sessionTimeout,
      heartbeatInterval: heartbeatInterval ?? this.options.heartbeatInterval,
      rebalanceTimeout: rebalanceTimeout ?? this.options.rebalanceTimeout,
      groupInstanceId: groupInstanceId ?? undefined,
      retry: {
        maxRetryTime: 30000,
        initialRetryTime: 1000,
        factor: 2,
        multiplier: 2,
        retries: 5,
        ...(options.retry || {}),
      },
    };

    const consumer = this.kafka.consumer(consumerOptions);

    // store the consumer instance keyed by its groupId
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
        payload,
      });
    });

    consumer.on("consumer.crash", (err) => {
      logger.error("Kafka consumer crash", { groupId, error: err });

      // Try to reconnect the specific tracked consumer with exponential backoff.
      const tracked = this.consumers.get(groupId);
      if (!tracked) return; // nothing to do if it's not tracked

      let retries = 0;
      const maxRetries = Math.max(5, this.options.maxConnectionRetries);

      const attempt = async () => {
        if (!this.consumers.has(groupId)) return; // stopped during shutdown
        try {
          logger.info("Attempting to reconnect Kafka consumer", {
            groupId,
            attempt: retries + 1,
          });
          await tracked.connect();
          logger.info("Kafka consumer reconnected", { groupId });
        } catch (e) {
          retries++;
          this.metrics.connectionRetries++;
          const delay = Math.min(1000 * 2 ** retries, 30_000);
          logger.warn("Kafka consumer reconnect failed, scheduling retry", {
            groupId,
            retries,
            delay,
            error: e,
          });
          setTimeout(attempt, delay);
        }
      };

      // kick off reconnect attempts
      setTimeout(attempt, 1000);
    });

    consumer.on("consumer.group_join", (info) => {
      logger.info("Consumer group join", { groupId, info });
    });

    return consumer;
  }

  public createProducer(overrides?: Partial<Parameters<Kafka["producer"]>[0]>) {
    const opts = {
      allowAutoTopicCreation: this.options.allowAutoTopicCreation,
      createPartitioner: this.options.createPartitioner,
      retry: {
        maxRetryTime:
          this.options.retryDelayMs * this.options.maxConnectionRetries,
        retries: this.options.maxConnectionRetries,
      },
      ...(overrides || {}),
    } as ProducerConfig;

    const p = this.kafka.producer(opts);

    return p;
  }

  public getProducer(): Producer {
    return this.producer;
  }

  public getMetrics(): KafkaMetrics {
    return { ...this.metrics };
  }

  public isReady(): boolean {
    return this.isConnected;
  }

  public async healthCheck(): Promise<boolean> {
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
