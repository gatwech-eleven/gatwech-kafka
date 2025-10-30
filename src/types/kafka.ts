import type { CompressionTypes, logLevel } from "kafkajs";

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
    correlationId?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type { ConsumerConfig } from "kafkajs";
