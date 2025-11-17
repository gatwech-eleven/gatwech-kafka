import { logger } from "../utils/logger";
import { Kafka, CompressionTypes } from "kafkajs";
import type {
  Producer,
  ConsumerConfig,
  Transaction,
  Admin,
  ProducerConfig,
} from "kafkajs";
import type { KafkaClientOptions, KafkaMetrics, TKafkaMessage } from "../types";
import { KafkaError } from "./errors";
import { DEFAULT_OPTIONS } from "./default-options";
import { createConsumerForClient } from "./create-consumer";

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
      idempotent: true,
      retry: {
        maxRetryTime:
          this.options.retryDelayMs * this.options.maxConnectionRetries,
        retries: this.options.maxConnectionRetries,
      },
    });

    this.setupEventListeners();

    // Optionally kick off a non-blocking connection on startup
    if (this.options.autoConnectAsync) {
      this.connectAsync();
    }
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

  private async initializeKafkaConnection(): Promise<void> {
    const timeoutMs =
      this.options.maxConnectAttemptTimeMs ?? this.options.brokerWaitTimeoutMs;
    const pollMs =
      this.options.connectPollIntervalMs ??
      this.options.brokerWaitPollIntervalMs;
    const timeoutAt = Date.now() + timeoutMs;
    const admin = this.getAdmin();
    try {
      await admin.connect();
      while (Date.now() < timeoutAt) {
        try {
          const info = await admin.describeCluster();
          if (info.controller) return; // ready
        } catch (err) {
          logger.warn("Broker readiness check failed; retrying", {
            error: err,
          });
        }
        await new Promise((r) => setTimeout(r, pollMs));
      }
      throw new KafkaError("Broker readiness timeout", undefined, {
        timeoutMs,
      });
    } finally {
      try {
        await admin.disconnect();
      } catch {}
    }
  }

  public async connect(): Promise<void> {
    if (this.isConnected) return;

    while (this.connectionRetries < this.options.maxConnectionRetries) {
      try {
        if (
          this.options.connectWithRetries ||
          this.options.connectAsync ||
          this.options.waitForBrokerBeforeConnect
        ) {
          await this.initializeKafkaConnection();
        }
        await this.producer.connect();
        // Mark ready after successful connect to avoid relying solely on events
        this.isConnected = true;
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
        acks: -1,
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
    return createConsumerForClient(
      this.kafka,
      this.options,
      groupId,
      options,
      this.consumers,
      this.metrics
    );
  }

  public createProducer(overrides?: Partial<Parameters<Kafka["producer"]>[0]>) {
    const opts = {
      allowAutoTopicCreation: this.options.allowAutoTopicCreation,
      createPartitioner: this.options.createPartitioner,
      idempotent: true,
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
