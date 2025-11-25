import { Partitioners, CompressionTypes, logLevel } from "kafkajs";
import type { KafkaClientOptions } from "../../types";
import type { Brand, DeepReadonly } from "../../types/utility-types";

/**
 * Represents the immutable, branded default configuration for the Kafka client.
 * * @remarks
 * This type is "Branded" to prevent it from being structurally compatible with 
 * partial or raw configuration objects, ensuring these defaults are explicitly 
 * handled as a complete set.
 */
export type DefaultOptions = Brand<
  DeepReadonly<KafkaClientOptions>,
  "DefaultOptions"
>;

/**
 * The standard default configuration values for the Kafka client service.
 * * These defaults represent a "sensible start" for most use cases, prioritizing
 * reliability (retries, timeouts) and moderate performance (GZIP compression).
 * * @constant
 * @type {DefaultOptions}
 */
export const DEFAULT_OPTIONS: DefaultOptions = {
  /**
   * Whether to allow the broker to automatically create topics if they 
   * do not exist when fetching metadata.
   */
  allowAutoTopicCreation: true,
  /**
   * The default partitioner to use for the producer. 
   * `DefaultPartitioner` usually maps messages to partitions based on the key hash.
   */
  createPartitioner: Partitioners.DefaultPartitioner,

  /** Level of logging verbosity. Defaults to INFO. */
  logLevel: logLevel.INFO,

  /** Whether to enable metrics collection for the client. */
  metricsEnabled: true,

  /** Maximum number of retries per connection attempt. */
  maxConnectionRetries: 5,

  /** Time in milliseconds to wait between retry attempts. */
  retryDelayMs: 2000,
  /** * The default compression codec. 
   * GZIP provides a good balance of compression ratio vs CPU usage.
   */
  compression: CompressionTypes.GZIP,

  /**
   * The maximum time that the group coordinator will wait for each member 
   * to rejoin when rebalancing. 
   * @default 60000 (1 minute)
   */
  rebalanceTimeout: 60000,
  /**
   * The timeout used to detect client failures. If the broker does not receive 
   * a heartbeat within this time, the member is removed from the group.
   * @default 30000 (30 seconds)
   */
  sessionTimeout: 30000,
  /**
   * The expected time between heartbeats to the consumer coordinator.
   * Must be lower than `sessionTimeout` (usually 1/3rd).
   * @default 3000 (3 seconds)
   */
  heartbeatInterval: 3000,

  /**
   * The maximum time to wait for a response from a broker before considering 
   * the request failed.
   */
  requestTimeout: 30000,
  /**
   * Timeout for the initial socket connection.
   */
  connectionTimeout: 1000,
  /**
   * Whether to enforce the request timeout on the client side.
   */
  enforceRequestTimeout: false,
  
  /** 
   * SSL configuration. Defaults to undefined (plaintext). 
   */
  ssl: undefined,

  /**
   * SASL authentication configuration. Defaults to undefined.
   */
  sasl: undefined,
  /**
   * Authentication timeout in milliseconds.
   */
  authenticationTimeout: undefined,
  /**
   * Reauthentication threshold in milliseconds.
   */
  reauthenticationThreshold: undefined,

  /**
   * Whether to attempt connection asynchronously upon initialization.
   */
  autoConnectAsync: false,
  /**
   * Whether to wrap the initial connection in a retry mechanism.
   */
  connectWithRetries: false,
  /**
   * Whether to execute the connect method without awaiting completion.
   */
  connectAsync: false,
  /**
   * Maximum time to spend attempting to connect before throwing.
   */
  maxConnectAttemptTimeMs: 180000,
  /**
   * Interval between connection polling attempts.
   */
  connectPollIntervalMs: 1000,
  /**
   * Maximum time to wait for brokers to become available in the wrapper logic.
   */
  brokerWaitTimeoutMs: 180000,
  /**
   * Polling interval while waiting for brokers.
   */
  brokerWaitPollIntervalMs: 1000,

} as const as DefaultOptions;