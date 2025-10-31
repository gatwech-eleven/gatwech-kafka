import { KafkaClient } from './client/index.cjs';
export { BaseProducer, ConnectRetryOptions, KafkaError, connectWithRetry, createEventPublisher } from './client/index.cjs';
export { TopicConfig, TopicGroups, TopicNames, chatTopics, notificationTopics, orderTopics, paymentTopics, topics, userTopics } from './config/topics/index.cjs';
export { TopicMap, ensureTopicsWithConfig } from './config/ensure-topics.cjs';
import { TKafkaMessage } from './types/index.cjs';
export { ISerializer, JsonSerializer, KafkaClientOptions, KafkaMetrics, WithMetadata } from './types/index.cjs';
import { z } from '@payvo/utils';
import { z as z$1 } from '@payvo/utils/zod';
export { Consumer, ConsumerConfig, Producer } from 'kafkajs';
import './topic-config-schema-CWgKTEuX.cjs';

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

type index$1_ValidationResult<T> = ValidationResult<T>;
declare const index$1_validateMessage: typeof validateMessage;
declare namespace index$1 {
  export { type index$1_ValidationResult as ValidationResult, _default$3 as default, index$1_validateMessage as validateMessage };
}

/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
declare function validateAndSend<T>(client: KafkaClient, topic: string, schema: z.ZodType<T>, message: TKafkaMessage<T>): Promise<void>;
/** Validate and send multiple messages. Any invalid message will abort sending. */
declare function validateAndSendMany<T>(client: KafkaClient, topic: string, schema: z.ZodType<T>, messages: TKafkaMessage<T>[]): Promise<void>;
declare const _default$2: {
    validateAndSend: typeof validateAndSend;
    validateAndSendMany: typeof validateAndSendMany;
};

declare const validateAndSend$1_validateAndSend: typeof validateAndSend;
declare const validateAndSend$1_validateAndSendMany: typeof validateAndSendMany;
declare namespace validateAndSend$1 {
  export { _default$2 as default, validateAndSend$1_validateAndSend as validateAndSend, validateAndSend$1_validateAndSendMany as validateAndSendMany };
}

declare const UserRegisteredSchema: any;
declare const ResetPasswordRequestEmailSchema: any;
declare const ResetPasswordRequestPhoneSchema: any;
declare const ResetPasswordRequestUnionSchema: any;
type TUserRegistered = z$1.infer<typeof UserRegisteredSchema>;
type TResetPasswordRequest = z$1.infer<typeof ResetPasswordRequestUnionSchema>;
declare const _default$1: {
    UserRegisteredSchema: any;
    ResetPasswordRequestEmailSchema: any;
    ResetPasswordRequestPhoneSchema: any;
    ResetPasswordRequestUnionSchema: any;
};

declare const NotificationEmailSentSchema: any;
type TNotificationEmailSent = z$1.infer<typeof NotificationEmailSentSchema>;
declare const _default: {
    NotificationEmailSentSchema: any;
};

declare const index_NotificationEmailSentSchema: typeof NotificationEmailSentSchema;
declare const index_ResetPasswordRequestEmailSchema: typeof ResetPasswordRequestEmailSchema;
declare const index_ResetPasswordRequestPhoneSchema: typeof ResetPasswordRequestPhoneSchema;
declare const index_ResetPasswordRequestUnionSchema: typeof ResetPasswordRequestUnionSchema;
type index_TNotificationEmailSent = TNotificationEmailSent;
type index_TResetPasswordRequest = TResetPasswordRequest;
type index_TUserRegistered = TUserRegistered;
declare const index_UserRegisteredSchema: typeof UserRegisteredSchema;
declare namespace index {
  export { index_NotificationEmailSentSchema as NotificationEmailSentSchema, index_ResetPasswordRequestEmailSchema as ResetPasswordRequestEmailSchema, index_ResetPasswordRequestPhoneSchema as ResetPasswordRequestPhoneSchema, index_ResetPasswordRequestUnionSchema as ResetPasswordRequestUnionSchema, type index_TNotificationEmailSent as TNotificationEmailSent, type index_TResetPasswordRequest as TResetPasswordRequest, type index_TUserRegistered as TUserRegistered, index_UserRegisteredSchema as UserRegisteredSchema, _default as notificationSchemas, _default$1 as userSchemas };
}

declare function resolveBrokersFromEnv(): string[];

declare const BaseProducerSchema: any;

export { BaseProducerSchema, KafkaClient, TKafkaMessage, validateAndSend$1 as producer, resolveBrokersFromEnv, index as schemas, index$1 as validation };
