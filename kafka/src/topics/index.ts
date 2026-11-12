import type { ZodType } from "@gatwech/utils/zod";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import type { Topic, TopicOptions, Topics } from "./types";

export type * from "./types";

const DEFAULT_RETENTION_MS = 604_800_000;

/**
 * Declares a service's topics
 * @description The way `schema.ts` declares a plugin's models: it describes
 * the topics and reads nothing from the environment. What differs between
 * environments, like the `namespace`, is an option of `createKafka`
 *
 * @example
 * export const topics = defineTopics({
 * 	userRegistered: {
 * 		name: "user.registered",
 * 		schema: z.object({ userId: z.string(), email: z.string() }),
 * 		key: (event) => event.userId,
 * 	},
 * });
 */
export function defineTopics<Schemas extends Record<string, ZodType>>(
	topics: {
		[Name in keyof Schemas]: TopicOptions<Schemas[Name]>;
	},
): Topics<Schemas> {
	const result: Record<string, Topic> = {};
	const identities = new Set<string>();

	for (const [key, options] of Object.entries<TopicOptions>(topics)) {
		if (typeof options.name !== "string" || options.name.length === 0) {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_NAME_REQUIRED, key);
		}
		if (typeof options.schema?.safeParse !== "function") {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_SCHEMA_REQUIRED, key);
		}
		if (typeof options.key !== "function") {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_KEY_REQUIRED, key);
		}
		for (const option of [
			"partitions",
			"replicationFactor",
			"retentionMs",
		] as const) {
			const value = options[option];
			if (value !== undefined && !(Number.isInteger(value) && value > 0)) {
				throw new KafkaError(
					KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
					`${key}.${option}`,
				);
			}
		}

		const namespaced = options.namespaced ?? true;
		// A namespaced and a bare topic of one name are different on the broker
		const identity = `${namespaced}:${options.name}`;
		if (identities.has(identity)) {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_NAME_DUPLICATED, key);
		}
		identities.add(identity);

		result[key] = Object.freeze({
			name: options.name,
			namespaced,
			schema: options.schema,
			key: options.key,
			partitions: options.partitions,
			replicationFactor: options.replicationFactor,
			retentionMs: options.retentionMs ?? DEFAULT_RETENTION_MS,
			cleanupPolicy: options.cleanupPolicy ?? "delete",
		});
	}

	return Object.freeze(result) as Topics<Schemas>;
}

/**
 * A topic's name on the broker: its declared name, with the `namespace` in
 * front unless the topic opts out
 */
export function resolveTopicName(
	topic: Pick<Topic, "name" | "namespaced">,
	namespace?: string | undefined,
) {
	return namespace && topic.namespaced
		? `${namespace}.${topic.name}`
		: topic.name;
}

/**
 * The name of a topic's DLQ when `dlq.topic` isn't set
 */
export function defaultDlqTopicName(
	topic: Pick<Topic, "name" | "namespaced">,
	namespace?: string | undefined,
) {
	return `${resolveTopicName(topic, namespace)}.dlq`;
}

/**
 * The topics as the broker knows them, with the namespace in their names
 */
export function resolveTopics<Resolved extends Record<string, Topic<any>>>(
	topics: Resolved,
	namespace?: string | undefined,
): Resolved {
	const resolved: Record<string, Topic> = {};
	for (const [key, topic] of Object.entries(topics)) {
		resolved[key] = Object.freeze({
			...topic,
			name: resolveTopicName(topic, namespace),
		});
	}
	return Object.freeze(resolved) as Resolved;
}
