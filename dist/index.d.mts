import { a as JsonSerializer, i as ISerializer, l as TKafkaMessage, n as ConsumerConfig, o as KafkaClientOptions, r as DLQOptions, s as KafkaMetrics, t as Consumer, u as WithMetadata } from "./index-Dewog11d.mjs";
import { _ as KafkaClient, a as EventPublisher, c as consumeWithRetryAndDLQ, d as CreatePublisherOptions, f as createEventPublisher, g as TReadyToPublishMessage, h as KeyDeriver, i as createDLQPublisher, l as ConnectRetryOptions, m as GenericKafkaProducer, n as DLQContext, o as createServicePublisherFactory, p as createPublisher, r as DLQMessageParts, s as ConsumeHandlerContext, t as CreateDLQPublisherOptions, u as connectWithRetry, v as KafkaError, y as BaseProducer } from "./index-Cu3zJToL.mjs";
import { n as TopicConfigSchema, t as TopicConfig } from "./topic-config-schema-B-xz1L2k.mjs";
import { n as ensureTopicsWithConfig, t as TopicMap } from "./ensure-topics-BEBRunmo.mjs";
import { a as notificationTopics, i as orderTopics, n as chatTopics, o as userTopics, r as paymentTopics, t as topics } from "./index-BC75ktkO.mjs";
import { Producer } from "kafkajs";
import { z } from "@gatwech/utils";
import { z as z$1 } from "@gatwech/utils/zod";
import { z as z$2 } from "zod";

//#region src/validation/index.d.ts
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
/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
declare function validateAndSend<T>(client: KafkaClient, topic: TopicConfig, message: TKafkaMessage<T>): Promise<void>;
/** Validate and send multiple messages. Any invalid message will abort sending. */
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
export { BaseProducer, BaseProducerSchema, ConnectRetryOptions, ConsumeHandlerContext, Consumer, ConsumerConfig, CreateDLQPublisherOptions, CreatePublisherOptions, DLQContext, DLQMessageParts, DLQOptions, EventPublisher, GenericKafkaProducer, ISerializer, JsonSerializer, KafkaClient, KafkaClientOptions, KafkaError, KafkaMetrics, KeyDeriver, type Producer, TKafkaMessage, TReadyToPublishMessage, TopicConfig, TopicConfigSchema, TopicMap, WithMetadata, chatTopics, connectWithRetry, consumeWithRetryAndDLQ, createDLQPublisher, createEventPublisher, createPublisher, createServicePublisherFactory, ensureTopicsWithConfig, notificationTopics, orderTopics, paymentTopics, validate_and_send_d_exports as producer, resolveBrokersFromEnv, index_d_exports as schemas, topics, userTopics, index_d_exports$1 as validation };