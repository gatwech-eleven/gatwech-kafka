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
  /**
   * When true, the client will invoke connectAsync() during construction,
   * allowing non-blocking startup. Defaults to false.
   */
  autoConnectAsync?: boolean;

  /**
   * Prefer this flag going forward. When true, KafkaClient.connect() performs
   * a readiness check with retries before establishing the producer connection.
   * Replaces the older `waitForBrokerBeforeConnect` name.
   */
  connectWithRetries?: boolean;

  /**
   * Alias: when true, behave like `connectWithRetries`. Name aligns with
   * intent of performing an async, retrying connect.
   */
  connectAsync?: boolean;

  /**
   * @deprecated Use `connectWithRetries` instead.
   * When true, KafkaClient.connect() waits for the broker to be ready
   * (describeCluster has a controller) before connecting the producer.
   * Defaults to false.
   */
  waitForBrokerBeforeConnect?: boolean;
  /** Total time to wait for broker readiness (ms). Default 30000. */
  maxConnectAttemptTimeMs?: number;
  /** Poll interval for broker readiness checks (ms). Default 500. */
  connectPollIntervalMs?: number;
  /** @deprecated Use `maxConnectAttemptTimeMs`. */
  brokerWaitTimeoutMs?: number;
  /** @deprecated Use `connectPollIntervalMs`. */
  brokerWaitPollIntervalMs?: number;
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
