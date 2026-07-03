import { type Producer } from "kafkajs";
import { logger } from "../../utils/logger";
import {
  type TKafkaMessage,
  type ISerializer,
  JsonSerializer,
  type KafkaMetrics,
} from "../../types";
import type { DeepReadonly } from "../../types/utility-types";
import { KafkaError } from "../core/errors";

export abstract class BaseProducer<T> {
  protected abstract readonly topic: string;
  private producer: Producer;
  private serializer: ISerializer<T>;
  private metrics: KafkaMetrics;

  constructor(
    producer: Producer,
    metrics?: KafkaMetrics,
    serializer: ISerializer<T> = new JsonSerializer<T>()
  ) {
    this.producer = producer;
    this.serializer = serializer;
    this.metrics = metrics || {
      publishSuccess: 0,
      publishFailures: 0,
      connectionRetries: 0,
      consumerConnections: 0,
    };
  }

  /**
   * Publishes a single message to the configured Kafka topic.
   * It handles serialization and logs the outcome.
   * @param data The Kafka message object to publish.
   * @returns A promise that resolves when the message has been successfully published.
   * @throws {KafkaError} if the publish operation fails.
   */
  async publish(data: TKafkaMessage<DeepReadonly<T>>): Promise<void> {
    try {
      logger.info(`Publishing to ${this.topic}`, { key: data.key });

      await this.producer.send({
        topic: this.topic,
        messages: [
          {
            key: data.key,
            // serializer expects mutable T; cast readonly->mutable for runtime
            value: this.serializer.serialize(data.value as unknown as T),
            headers: data.headers,
            timestamp: data.timestamp,
          },
        ],
      });

      this.metrics.publishSuccess++;
      logger.debug(`Message published to ${this.topic}`);
    } catch (error) {
      this.metrics.publishFailures++;
      logger.error(`Publish failed to ${this.topic}`, error);
      throw new KafkaError(
        `Failed to publish to ${this.topic}`,
        error as Error,
        {
          key: data.key,
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
  async publishBatch(
    messages: TKafkaMessage<DeepReadonly<T>>[]
  ): Promise<void> {
    try {
      await this.producer.send({
        topic: this.topic,
        messages: messages.map((msg) => ({
          key: msg.key,
          value: this.serializer.serialize(msg.value as unknown as T),
          headers: msg.headers,
          timestamp: msg.timestamp,
        })),
      });
      this.metrics.publishSuccess += messages.length;
    } catch (error) {
      this.metrics.publishFailures += messages.length;
      throw new KafkaError(
        `Batch publish failed to ${this.topic}`,
        error as Error,
        {
          messageCount: messages.length,
        }
      );
    }
  }
}
