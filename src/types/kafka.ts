import type { CompressionTypes, logLevel, SASLOptions, Mechanism } from "kafkajs";
import type { ConnectionOptions } from "tls";
import type { CorrelationId } from "./utility-types";
import type { TopicConfig, ValidatedTopicConfig } from "../config/topics/topic-config-schema";

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
  ssl?: boolean | ConnectionOptions;
  sasl?: SASLOptions | Mechanism;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  reauthenticationThreshold?: number;
  enforceRequestTimeout?: boolean;
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

  /** Total time to wait for broker readiness (ms). Default 30000. */
  maxConnectAttemptTimeMs?: number;
  /** Poll interval for broker readiness checks (ms). Default 500. */

  connectPollIntervalMs?: number;
  /** @deprecated Use `maxConnectAttemptTimeMs`. */
  
  brokerWaitTimeoutMs?: number;
  /** @deprecated Use `connectPollIntervalMs`. */

  brokerWaitPollIntervalMs?: number;
  
  /** 
   * Default metadata to include in all messages sent by this client. Message-level metadata takes precedence. 
   */
  defaultMetadata?: Record<string, unknown>;


  /** 
   * Default headers to include in all messages sent by this client. Message-level headers take precedence. 
   */
  defaultHeaders?: Record<string, string | Buffer>;

  /** 
   * Topic configurations for this client. Accessible via client.topics 
   */
  topics?: Record<string, unknown>;
  /** 
   * Resilience configuration options 
   */
  resilience?: ResilienceOptions;
  /** 
   * Topic provisioning configuration options 
   */
  topicProvisioning?: TopicProvisioningOptions;
}

export interface TopicProvisioningOptions {
  /**
   * Default number of partitions for new topics
   */
  partitions?: number;
  /**
   * Default replication factor for new topics
   */
  replicationFactor?: number;
  /**
   * Max time to wait for leaders after create/partition increase
   */
  waitForLeadersMs?: number;
}

export interface RetryOptions {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
}

export interface ReconnectionOptions {
  enabled: boolean;
  intervalMs: number;
  onReconnect?: () => Promise<void>;
}

export interface CircuitBreakerOptions {
  enabled: boolean;
  failureThreshold: number;
  /**
   * Time in milliseconds to wait before allowing a test request (Half-Open state)
   * @default 30000 (30 seconds)
   */
  resetTimeoutMs: number;
}

export interface ResilienceOptions {
  enabled: boolean;
  retry?: Partial<RetryOptions>;
  reconnection?: Partial<ReconnectionOptions>;
  circuitBreaker?: Partial<CircuitBreakerOptions>;
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

/**
 * Logger interface for custom logging implementations.
 * Allows applications to provide their own logger to the Kafka client.
 */
export interface Logger {
  info: (message: string, meta?: any) => void;
  warn: (message: string, meta?: any) => void;
  error: (message: string, meta?: any) => void;
}

export interface DLQOptions {
  dlqEnabled?: boolean;
  /** Number of total attempts (1 original + retries - 1). Default 3 */
  maxRetries?: number;
  /** Initial backoff in ms for exponential backoff. Default 500 */
  retryBackoffMs?: number;
  /** Suffix appended to original topic for DLQ topic name. Default ".dlq" */
  dlqSuffix?: string;
}

