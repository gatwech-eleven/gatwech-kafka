import type { ZodType, z } from "@gatwech/utils/zod";
import type { EachMessagePayload } from "kafkajs";
import type { TopicConfig } from "../config/topics/topic-config-schema";
import type { DLQOptions } from "./kafka";

/**
 * Payload type inferred from a Zod schema.
 */
export type SchemaConsumerPayload<TSchema extends ZodType> = z.infer<TSchema>;

/**
 * Raw KafkaJS payload for topics without a schema.
 */
export type RawConsumerPayload = EachMessagePayload;

/**
 * Alias for DLQ options used in consumer configuration.
 */
export type ConsumerDlqOptions = DLQOptions;

/**
 * Definition for a topic that explicitly does not have a schema.
 * Used for raw consumption.
 */
export interface RawTopic {
	name: string;
	schema?: undefined | null;
}

/**
 * Union type representing any valid topic configuration for consumption.
 * Can be a strongly-typed TopicConfig or a RawTopic.
 */
export type AnyTopic = TopicConfig | RawTopic;
