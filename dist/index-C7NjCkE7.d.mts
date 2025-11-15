import { CompressionTypes, Consumer, ConsumerConfig as ConsumerConfig$1, Producer as Producer$1, logLevel } from "kafkajs";

//#region src/types/kafka.d.ts
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
//#endregion
export { KafkaClientOptions as a, TKafkaMessage as c, JsonSerializer as i, WithMetadata as l, ConsumerConfig$1 as n, KafkaMetrics as o, ISerializer as r, Producer$1 as s, Consumer as t };