import { Partitioners, CompressionTypes, logLevel } from "kafkajs";
import type { KafkaClientOptions } from "../types";
import type { Brand, DeepReadonly } from "../types/utility-types";

export type DefaultOptions = Brand<
  DeepReadonly<Required<KafkaClientOptions>>,
  "DefaultOptions"
>;

export const DEFAULT_OPTIONS = {
  allowAutoTopicCreation: true,
  createPartitioner: Partitioners.DefaultPartitioner,
  maxConnectionRetries: 5,
  retryDelayMs: 2000,
  compression: CompressionTypes.GZIP,
  metricsEnabled: true,
  logLevel: logLevel.INFO,
  rebalanceTimeout: 60000,
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  requestTimeout: 30000,
  autoConnectAsync: false,
  connectWithRetries: false,
  connectAsync: false,
  waitForBrokerBeforeConnect: false,
  maxConnectAttemptTimeMs: 180000,
  connectPollIntervalMs: 1000,
  brokerWaitTimeoutMs: 180000,
  brokerWaitPollIntervalMs: 1000,
} as const as DefaultOptions;
