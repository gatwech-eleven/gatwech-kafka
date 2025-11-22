import "./utility-types-DYlGDM0l.mjs";
import { a as JsonSerializer, i as ISerializer, l as TKafkaMessage, n as ConsumerConfig, o as KafkaClientOptions, r as DLQOptions, s as KafkaMetrics, t as Consumer, u as WithMetadata } from "./index-ze4GS2TY.mjs";
import { C as BaseProducer, S as KafkaClient, _ as connectWithRetry, a as ConsumeHandlerContext, b as ModularKafkaClient, c as createServicePublisherFactory, d as createPublisher, f as GenericKafkaProducer, g as ConnectRetryOptions, h as TReadyToPublishMessage, i as createDLQPublisher, l as CreatePublisherOptions, m as PartitionKeyFn, n as DLQContext, o as consumeWithRetryAndDLQ, p as KeyDeriver, r as DLQMessageParts, s as EventPublisher, t as CreateDLQPublisherOptions, u as createEventPublisher, v as CreateKafkaClientConfig, x as createKafkaClient, y as KafkaExtension } from "./index-Dz00U3Cw.mjs";
import { i as ValidatedTopicConfig, n as TopicConfigInput, r as TopicConfigSchema, t as TopicConfig } from "./topic-config-schema-DBIkyeN0.mjs";
import { n as ensureTopicsWithConfig, r as initializeKafkaTopics, t as TopicMap } from "./ensure-topics-CLVKvruU.mjs";
import { a as createTopicConfig, c as topicsToArray, d as orderTopics, f as notificationTopics, i as createSingleTopicConfig, l as chatTopics, n as TopicConfigBuilder, o as defineTopic, p as userTopics, r as topicBuilder, s as defineTopics, t as topics, u as paymentTopics } from "./index-CJYhaXat.mjs";
import { Producer } from "kafkajs";
import { z } from "@gatwech/utils";
import { z as z$1 } from "@gatwech/utils/zod";
import { z as z$2 } from "zod";

//#region src/extensions/topic-provisioning.d.ts
interface TopicProvisioningOptions {
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
  /**
   * Whether to include built-in package topics
   */
  includeBuiltIn?: boolean;
}
/**
 * Extension that adds topic provisioning capabilities to the Kafka client.
 *
 * @param topics - Map of topic configurations (from createTopicConfig)
 * @param options - Provisioning options
 */
declare function topicProvisioningExtension(topics?: Record<string, ValidatedTopicConfig | TopicConfig>, options?: TopicProvisioningOptions): KafkaExtension<{
  provisionTopics: () => Promise<void>;
}>;
//#endregion
//#region src/extensions/health-check.d.ts
/**
 * Extension that provides health check functionality.
 *
 * While the base client has a healthCheck method, this extension allows
 * for more consistent access patterns in the modular architecture.
 */
declare function healthCheckExtension(): KafkaExtension<{
  healthCheck: () => Promise<boolean>;
}>;
//#endregion
//#region src/extensions/partition-calc.d.ts
/** Defines the throughput metrics needed for the 4-Pillar calculation. */
interface ThroughputMetrics {
  /** Target total throughput (T) in MB/s. */
  targetThroughputMBps: number;
  /** Producer throughput per partition (P) in MB/s. */
  producerPerPartitionMBps: number;
  /** Consumer throughput per partition (C) in MB/s (slower part). */
  consumerPerPartitionMBps: number;
  /** Desired consumer parallelism (max active instances). */
  desiredParallelism: number;
  /** Projected growth factor (e.g., 2.0 for 100% growth). */
  growthFactor: number;
}
interface PartitionSkewResult {
  status: 'BALANCED' | 'SKEWED';
  hotPartitions: string[];
}
/**
 * Calculates the optimal number of partitions based on the 4-Pillar Framework
 * derived from the video: Throughput, Latency, Consumer Parallelism, and Future Growth.
 *
 * @throws Error if any metric is negative.
 */
declare const calculateOptimalPartitions: (metrics: ThroughputMetrics) => number;
/**
 * CLIENT EXTENSION: Adds partition analysis and calculation utilities to the Kafka Client.
 */
declare const partitionAnalysisClient: () => KafkaExtension<{
  calculatePartitions: typeof calculateOptimalPartitions;
  checkPartitionSkew: (distribution: number[], skewThreshold?: number) => PartitionSkewResult;
}>;
declare namespace index_d_exports$1 {
  export { ValidationResult, _default$3 as default, validateMessage };
}
type ValidationResult<T> = {
  success: true;
  data: T;
} | {
  success: false;
  error: ReturnType<typeof z.treeifyError>;
};
declare function validateMessage<T>(schema: z.ZodType<T>, value: unknown, context?: Record<string, unknown>): ValidationResult<T>;
declare const _default$3: {
  validateMessage: typeof validateMessage;
};
declare namespace validate_and_send_d_exports {
  export { _default$2 as default, validateAndSend, validateAndSendMany };
}
/**
 * @deprecated Use `createServicePublisher` (preferred) or `createEventPublisher`.
 * This helper performs schema validation then sends via a generic producer,
 * but the service/event factory APIs provide richer metadata injection and clearer contracts.
 */
declare function validateAndSend<T>(client: KafkaClient, topic: TopicConfig, message: TKafkaMessage<T>): Promise<void>;
/** Validate and send multiple messages. Any invalid message will abort sending. */
/**
 * @deprecated Use `createServicePublisher` (preferred) or `createEventPublisher`.
 * Validates each message and publishes as a batch. Prefer defining per-topic publishers
 * with factories for stronger typing, metadata enrichment, and observability.
 */
declare function validateAndSendMany<T>(client: KafkaClient, topic: TopicConfig, messages: TKafkaMessage<T>[]): Promise<void>;
declare const _default$2: {
  validateAndSend: typeof validateAndSend;
  validateAndSendMany: typeof validateAndSendMany;
};
//#endregion
//#region src/schemas/base.schema.d.ts
declare const BaseEventMetadataSchema: z$1.ZodOptional<z$1.ZodObject<{
  correlationId: z$1.ZodOptional<z$1.ZodString>;
  source: z$1.ZodOptional<z$1.ZodString>;
}, z$1.core.$strip>>;
declare const BaseProducerSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
}, z$1.core.$strip>;
//#endregion
//#region src/schemas/user-schemas.d.ts
declare const UserRegisteredSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  id: z$1.ZodString;
  email: z$1.ZodEmail;
  name: z$1.ZodOptional<z$1.ZodString>;
  otp: z$1.ZodOptional<z$1.ZodString>;
}, z$1.core.$strip>;
declare const ResetPasswordRequestEmailSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  userId: z$1.ZodOptional<z$1.ZodUUID>;
  identifier: z$1.ZodLiteral<"email">;
  email: z$1.ZodEmail;
  token: z$1.ZodString;
}, z$1.core.$strip>;
declare const ResetPasswordRequestPhoneSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  userId: z$1.ZodOptional<z$1.ZodUUID>;
  identifier: z$1.ZodLiteral<"phone">;
  phoneNumber: z$1.ZodString;
  otp: z$1.ZodOptional<z$1.ZodString>;
  token: z$1.ZodOptional<z$1.ZodString>;
}, z$1.core.$strip>;
declare const ResetPasswordRequestUnionSchema: z$1.ZodUnion<readonly [z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  userId: z$1.ZodOptional<z$1.ZodUUID>;
  identifier: z$1.ZodLiteral<"email">;
  email: z$1.ZodEmail;
  token: z$1.ZodString;
}, z$1.core.$strip>, z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  userId: z$1.ZodOptional<z$1.ZodUUID>;
  identifier: z$1.ZodLiteral<"phone">;
  phoneNumber: z$1.ZodString;
  otp: z$1.ZodOptional<z$1.ZodString>;
  token: z$1.ZodOptional<z$1.ZodString>;
}, z$1.core.$strip>]>;
type TUserRegistered = z$1.infer<typeof UserRegisteredSchema>;
type TResetPasswordRequest = z$1.infer<typeof ResetPasswordRequestUnionSchema>;
declare const _default: {
  UserRegisteredSchema: z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    id: z$1.ZodString;
    email: z$1.ZodEmail;
    name: z$1.ZodOptional<z$1.ZodString>;
    otp: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>;
  ResetPasswordRequestEmailSchema: z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    userId: z$1.ZodOptional<z$1.ZodUUID>;
    identifier: z$1.ZodLiteral<"email">;
    email: z$1.ZodEmail;
    token: z$1.ZodString;
  }, z$1.core.$strip>;
  ResetPasswordRequestPhoneSchema: z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    userId: z$1.ZodOptional<z$1.ZodUUID>;
    identifier: z$1.ZodLiteral<"phone">;
    phoneNumber: z$1.ZodString;
    otp: z$1.ZodOptional<z$1.ZodString>;
    token: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>;
  ResetPasswordRequestUnionSchema: z$1.ZodUnion<readonly [z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    userId: z$1.ZodOptional<z$1.ZodUUID>;
    identifier: z$1.ZodLiteral<"email">;
    email: z$1.ZodEmail;
    token: z$1.ZodString;
  }, z$1.core.$strip>, z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    userId: z$1.ZodOptional<z$1.ZodUUID>;
    identifier: z$1.ZodLiteral<"phone">;
    phoneNumber: z$1.ZodString;
    otp: z$1.ZodOptional<z$1.ZodString>;
    token: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>]>;
};
//#endregion
//#region src/schemas/notification-schemas.d.ts
declare const NotificationEmailSentSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  to: z$1.ZodEmail;
  subject: z$1.ZodString;
  body: z$1.ZodString;
}, z$1.core.$strip>;
type TNotificationEmailSent = z$1.infer<typeof NotificationEmailSentSchema>;
declare const _default$1: {
  NotificationEmailSentSchema: z$1.ZodObject<{
    timestamp: z$1.ZodNumber;
    metadata: z$1.ZodOptional<z$1.ZodObject<{
      correlationId: z$1.ZodOptional<z$1.ZodString>;
      source: z$1.ZodOptional<z$1.ZodString>;
    }, z$1.core.$strip>>;
    to: z$1.ZodEmail;
    subject: z$1.ZodString;
    body: z$1.ZodString;
  }, z$1.core.$strip>;
};
//#endregion
//#region src/schemas/order-schemas.d.ts
declare const OrderCreatedSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  id: z$2.ZodString;
  userId: z$2.ZodString;
  amount: z$2.ZodNumber;
  items: z$2.ZodArray<z$2.ZodObject<{
    productId: z$2.ZodString;
    quantity: z$2.ZodNumber;
    price: z$2.ZodNumber;
  }, z$2.core.$strip>>;
}, z$2.core.$strip>;
declare const OrderUpdatedSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  id: z$2.ZodString;
  status: z$2.ZodString;
}, z$2.core.$strip>;
declare const OrderCancelledSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  id: z$2.ZodString;
  reason: z$2.ZodOptional<z$2.ZodString>;
}, z$2.core.$strip>;
type TOrderCreated = z$2.infer<typeof OrderCreatedSchema>;
type TOrderUpdated = z$2.infer<typeof OrderUpdatedSchema>;
type TOrderCancelled = z$2.infer<typeof OrderCancelledSchema>;
//#endregion
//#region src/schemas/payment-schemas.d.ts
declare const PaymentInitiatedSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  orderId: z$2.ZodString;
  amount: z$2.ZodNumber;
  method: z$2.ZodString;
}, z$2.core.$strip>;
declare const PaymentCompletedSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  transactionId: z$2.ZodString;
  orderId: z$2.ZodString;
  amount: z$2.ZodNumber;
}, z$2.core.$strip>;
declare const PaymentFailedSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  orderId: z$2.ZodString;
  reason: z$2.ZodString;
}, z$2.core.$strip>;
type TPaymentInitiated = z$2.infer<typeof PaymentInitiatedSchema>;
type TPaymentCompleted = z$2.infer<typeof PaymentCompletedSchema>;
type TPaymentFailed = z$2.infer<typeof PaymentFailedSchema>;
//#endregion
//#region src/schemas/chat-schemas.d.ts
declare const ChatMessageSentSchema: z$2.ZodObject<{
  timestamp: z$2.ZodNumber;
  metadata: z$2.ZodOptional<z$2.ZodObject<{
    correlationId: z$2.ZodOptional<z$2.ZodString>;
    source: z$2.ZodOptional<z$2.ZodString>;
  }, z$2.core.$strip>>;
  chatId: z$2.ZodString;
  userId: z$2.ZodString;
  message: z$2.ZodString;
}, z$2.core.$strip>;
type TChatMessageSent = z$2.infer<typeof ChatMessageSentSchema>;
//#endregion
//#region src/schemas/dlq.schema.d.ts
declare const DlqMessageSchema: z$1.ZodObject<{
  timestamp: z$1.ZodNumber;
  metadata: z$1.ZodOptional<z$1.ZodObject<{
    correlationId: z$1.ZodOptional<z$1.ZodString>;
    source: z$1.ZodOptional<z$1.ZodString>;
  }, z$1.core.$strip>>;
  originalTopic: z$1.ZodString;
  originalPartition: z$1.ZodNumber;
  originalOffset: z$1.ZodString;
  consumerGroup: z$1.ZodString;
  errorMessage: z$1.ZodString;
  stackTrace: z$1.ZodOptional<z$1.ZodString>;
  originalKey: z$1.ZodOptional<z$1.ZodString>;
  originalHeaders: z$1.ZodOptional<z$1.ZodRecord<z$1.ZodString, z$1.ZodString>>;
  originalValue: z$1.ZodAny;
}, z$1.core.$strip>;
type TDlqMessage = z$1.infer<typeof DlqMessageSchema>;
declare namespace index_d_exports {
  export { BaseEventMetadataSchema, BaseProducerSchema, ChatMessageSentSchema, DlqMessageSchema, NotificationEmailSentSchema, OrderCancelledSchema, OrderCreatedSchema, OrderUpdatedSchema, PaymentCompletedSchema, PaymentFailedSchema, PaymentInitiatedSchema, ResetPasswordRequestEmailSchema, ResetPasswordRequestPhoneSchema, ResetPasswordRequestUnionSchema, TChatMessageSent, TDlqMessage, TNotificationEmailSent, TOrderCancelled, TOrderCreated, TOrderUpdated, TPaymentCompleted, TPaymentFailed, TPaymentInitiated, TResetPasswordRequest, TUserRegistered, UserRegisteredSchema, _default$1 as notificationSchemas, _default as userSchemas };
}
//#endregion
//#region src/utils/brokers.d.ts
declare function resolveBrokersFromEnv(): string[];
//#endregion
export { BaseProducer, BaseProducerSchema, ConnectRetryOptions, ConsumeHandlerContext, Consumer, ConsumerConfig, CreateDLQPublisherOptions, CreateKafkaClientConfig, CreatePublisherOptions, DLQContext, DLQMessageParts, DLQOptions, EventPublisher, GenericKafkaProducer, ISerializer, JsonSerializer, KafkaClient, KafkaClientOptions, KafkaExtension, KafkaMetrics, KeyDeriver, ModularKafkaClient, type PartitionKeyFn, PartitionSkewResult, type Producer, TKafkaMessage, TReadyToPublishMessage, ThroughputMetrics, type TopicConfig, TopicConfigBuilder, type TopicConfigInput, TopicConfigSchema, TopicMap, TopicProvisioningOptions, type ValidatedTopicConfig, WithMetadata, calculateOptimalPartitions, chatTopics, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createKafkaClient, createPublisher, createServicePublisherFactory, createSingleTopicConfig, createTopicConfig, defineTopic, defineTopics, ensureTopicsWithConfig, healthCheckExtension, initializeKafkaTopics, notificationTopics, orderTopics, partitionAnalysisClient, paymentTopics, validate_and_send_d_exports as producer, resolveBrokersFromEnv, index_d_exports as schemas, topicBuilder, topicProvisioningExtension, topics, topicsToArray, userTopics, index_d_exports$1 as validation };