import { Producer } from 'kafkajs';
import { KafkaMetrics, ISerializer, TKafkaMessage } from './types.js';
export { K as KafkaClient, a as KafkaError } from './shared/kafka.BmhNfoBx.js';
export { createEventPublisher } from './client/event.factory.js';
import '@payvo/utils';

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

export { BaseProducer, connectWithRetry };
export type { ConnectRetryOptions };
