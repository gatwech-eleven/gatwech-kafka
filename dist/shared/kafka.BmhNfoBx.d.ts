import { Transaction, CompressionTypes, Admin, ConsumerConfig, Kafka, Producer } from 'kafkajs';
import { KafkaClientOptions, TKafkaMessage, KafkaMetrics } from '../types.js';

declare class KafkaError extends Error {
    readonly cause?: unknown;
    readonly meta?: Record<string, unknown>;
    constructor(message: string, cause?: unknown, meta?: Record<string, unknown>);
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
    createProducer(overrides?: Partial<Parameters<Kafka["producer"]>[0]>): any;
    getProducer(): Producer;
    getMetrics(): KafkaMetrics;
    isReady(): boolean;
    healthCheck(): Promise<boolean>;
}

export { KafkaClient as K, KafkaError as a };
