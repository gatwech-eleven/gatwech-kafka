import { a as KafkaClientOptions, c as TKafkaMessage, o as KafkaMetrics, r as ISerializer } from "./index-yRuFPNLk.cjs";
import { Admin, CompressionTypes, ConsumerConfig, Kafka, Producer, Transaction } from "kafkajs";
import { z } from "@gatwech/utils";

//#region src/client/base.d.ts
declare abstract class BaseProducer<T> {
  protected abstract readonly topic: string;
  private producer;
  private serializer;
  private metrics;
  constructor(producer: Producer, metrics?: KafkaMetrics, serializer?: ISerializer<T>);
  /**
   * Publishes a single message to the configured Kafka topic.
   * It handles serialization and logs the outcome.
   * @param data The Kafka message object to publish.
   * @returns A promise that resolves when the message has been successfully published.
   * @throws {KafkaError} if the publish operation fails.
   */
  publish(data: TKafkaMessage<T>): Promise<void>;
  /**
   * Publishes a batch of messages to the configured Kafka topic.
   * This is more efficient for sending multiple messages at once.
   * @param messages An array of Kafka messages to publish.
   * @returns A promise that resolves when the batch has been successfully published.
   * @throws {KafkaError} if the batch publish operation fails.
   */
  publishBatch(messages: TKafkaMessage<T>[]): Promise<void>;
}
//#endregion
//#region src/client/kafka-client.d.ts
declare class KafkaError extends Error {
  readonly cause?: unknown | undefined;
  readonly meta?: Record<string, unknown> | undefined;
  constructor(message: string, cause?: unknown | undefined, meta?: Record<string, unknown> | undefined);
}
declare class KafkaClient {
  private readonly producer;
  private readonly kafka;
  private readonly options;
  private isConnected;
  private connectionRetries;
  private metrics;
  private admin?;
  private consumers;
  constructor(clientId: string, brokers: string[], options?: KafkaClientOptions);
  private setupEventListeners;
  connect(): Promise<void>;
  connectAsync(): void;
  disconnect(): Promise<void>;
  runTransaction<T>(callback: (transaction: Transaction) => Promise<T>): Promise<T>;
  sendMessages<T>(topic: string, messages: TKafkaMessage<T>[], compression?: CompressionTypes): Promise<void>;
  sendMessage<T>(topic: string, message: TKafkaMessage<T>, compression?: CompressionTypes): Promise<void>;
  getAdmin(): Admin;
  createConsumer(groupId: string, options: ConsumerConfig): Promise<ReturnType<Kafka["consumer"]>>;
  createProducer(overrides?: Partial<Parameters<Kafka["producer"]>[0]>): Producer;
  getProducer(): Producer;
  getMetrics(): KafkaMetrics;
  isReady(): boolean;
  healthCheck(): Promise<boolean>;
}
//#endregion
//#region src/client/event.factory.d.ts
type KeyDeriver<T> = (payload: T) => string;
/**
 * Factory for creating validated event publishers.
 */
declare function createEventPublisher<TSchema extends z.ZodObject<any, any>>(producer: Producer, topicName: string, schema: TSchema, keyDeriver: KeyDeriver<z.infer<TSchema>>, source: string): (payload: z.input<TSchema>) => Promise<void>;
//#endregion
//#region src/client/connect-with-retry.d.ts
interface ConnectRetryOptions {
  maxAttempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
}
/**
 * Generic helper to retry an async connect function with exponential backoff.
 * Detects transient DNS errors (EAI_AGAIN) and will keep retrying up to maxAttempts.
 */
declare function connectWithRetry(connectFn: () => Promise<void>, opts?: ConnectRetryOptions): Promise<void>;
//#endregion
export { KafkaError as a, KafkaClient as i, connectWithRetry as n, BaseProducer as o, createEventPublisher as r, ConnectRetryOptions as t };