export { BaseProducer, ConnectRetryOptions, connectWithRetry } from './client.mjs';
export { K as KafkaClient, a as KafkaError } from './shared/kafka.Rlv8erdm.mjs';
export { createEventPublisher } from './client/event.factory.mjs';
export { TopicConfig, TopicGroups, TopicNames, chatTopics, notificationTopics, orderTopics, paymentTopics, topics, userTopics } from './config/topics.mjs';
export { TopicMap, ensureTopicsWithConfig } from './config/ensure-topics.mjs';
export { ISerializer, JsonSerializer, KafkaClientOptions, KafkaMetrics, TKafkaMessage, WithMetadata } from './types.mjs';
export { i as validation } from './shared/kafka.YlKgWoCJ.mjs';
export { v as producer } from './shared/kafka.D8Hj25-f.mjs';
export { i as schemas } from './shared/kafka.Di0Jm4o8.mjs';
export { ConsumerConfig, Producer } from 'kafkajs';
import '@payvo/utils';
import '@payvo/utils/zod';
import './shared/kafka.TtePgHi5.mjs';
import 'zod';

declare function resolveBrokersFromEnv(): string[];

declare const BaseProducerSchema: any;

export { BaseProducerSchema, resolveBrokersFromEnv };
