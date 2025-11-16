import type { CompressionTypes, logLevel } from "kafkajs";
import type { CorrelationId } from "./utility-types";

export interface KafkaClientOptions {
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

export interface KafkaMetrics {
  publishSuccess: number;
  publishFailures: number;
  connectionRetries: number;
  consumerConnections: number;
}

export interface ISerializer<T> {
  serialize(value: T): string;
  deserialize(value: string): T;
}

export class JsonSerializer<T> implements ISerializer<T> {
  serialize(value: T): string {
    return JSON.stringify(value);
  }

  deserialize(value: string): T {
    return JSON.parse(value);
  }
}

export interface TKafkaMessage<T> {
  key: string;
  value: T;
  headers?: Record<string, string | Buffer>;
  timestamp?: string;
  metadata?: WithMetadata["metadata"];
  partition?: number;
}

export type WithMetadata = {
  metadata?: {
    correlationId?: CorrelationId;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type { ConsumerConfig, Consumer, Producer } from "kafkajs";

export interface DLQOptions {
  dlqEnabled?: boolean;
  /** Number of total attempts (1 original + retries - 1). Default 3 */
  maxRetries?: number;
  /** Initial backoff in ms for exponential backoff. Default 500 */
  retryBackoffMs?: number;
  /** Suffix appended to original topic for DLQ topic name. Default ".dlq" */
  dlqSuffix?: string;
}
