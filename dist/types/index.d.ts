import { CompressionTypes, logLevel } from 'kafkajs';
export { Consumer, ConsumerConfig } from 'kafkajs';

interface KafkaClientOptions {
    allowAutoTopicCreation?: boolean;
    createPartitioner?: () => (args: any) => number;
    maxConnectionRetries?: number;
    retryDelayMs?: number;
    compression?: CompressionTypes;
    metricsEnabled?: boolean;
    logLevel?: logLevel;
    rebalanceTimeout?: number;
    sessionTimeout?: number;
    heartbeatInterval?: number;
    requestTimeout?: number;
}
interface KafkaMetrics {
    publishSuccess: number;
    publishFailures: number;
    connectionRetries: number;
    consumerConnections: number;
}
interface ISerializer<T> {
    serialize(value: T): string;
    deserialize(value: string): T;
}
declare class JsonSerializer<T> implements ISerializer<T> {
    serialize(value: T): string;
    deserialize(value: string): T;
}
interface TKafkaMessage<T> {
    key: string;
    value: T;
    headers?: Record<string, string | Buffer>;
    timestamp?: string;
    metadata?: WithMetadata["metadata"];
    partition?: number;
}
type WithMetadata = {
    metadata?: {
        correlationId?: string;
        [key: string]: unknown;
    };
    [key: string]: unknown;
};

export { type ISerializer, JsonSerializer, type KafkaClientOptions, type KafkaMetrics, type TKafkaMessage, type WithMetadata };
