export { BaseProducer, ConnectRetryOptions, connectWithRetry } from './client.js';
export { K as KafkaClient, a as KafkaError } from './shared/kafka.BmhNfoBx.js';
export { createEventPublisher } from './client/event.factory.js';
export { TopicConfig, TopicGroups, TopicNames, chatTopics, notificationTopics, orderTopics, paymentTopics, topics, userTopics } from './config/topics.js';
export { TopicMap, ensureTopicsWithConfig } from './config/ensure-topics.js';
export { ISerializer, JsonSerializer, KafkaClientOptions, KafkaMetrics, TKafkaMessage, WithMetadata } from './types.js';
export { i as validation } from './shared/kafka.YlKgWoCJ.js';
export { v as producer } from './shared/kafka.DrZLaGhv.js';
export { i as schemas } from './shared/kafka.Di0Jm4o8.js';
export { ConsumerConfig, Producer } from 'kafkajs';
import '@payvo/utils';
import '@payvo/utils/zod';
import './shared/kafka.TtePgHi5.js';
import 'zod';

declare function resolveBrokersFromEnv(): string[];

declare const BaseProducerSchema: any;

export { BaseProducerSchema, resolveBrokersFromEnv };
