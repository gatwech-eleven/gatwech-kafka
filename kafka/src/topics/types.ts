import type { z, ZodType } from "@gatwech/utils/zod";

export interface TopicOptions<Schema extends ZodType = ZodType> {
	/**
	 * The topic's name on the broker
	 *
	 * @example "auth.user.registered"
	 */
	name: string;
	/**
	 * What an event on this topic looks like
	 * @description Publishers refuse an event it rejects, and consumers send a
	 * message it rejects to the DLQ
	 */
	schema: Schema;
	/**
	 * The partition key of an event
	 * @description Events with the same key sit on one partition, so they are
	 * read in the order they were published
	 *
	 * @example (event) => event.userId
	 */
	key: (event: z.output<Schema>) => string;
	/**
	 * How many partitions the topic has
	 * @description More partitions let more consumers of a group read at once
	 *
	 * @default provisioning.partitions
	 */
	partitions?: number | undefined;
	/**
	 * How many brokers keep a copy of each partition
	 *
	 * @default provisioning.replicationFactor
	 */
	replicationFactor?: number | undefined;
	/**
	 * How long a message is kept, in milliseconds
	 * @description Ignored when `cleanupPolicy` is `"compact"`
	 *
	 * @default 604_800_000, 7 days
	 */
	retentionMs?: number | undefined;
	/**
	 * What the broker does with old messages
	 * @description `"delete"` drops them after `retentionMs`. `"compact"` keeps
	 * the latest message of each key
	 *
	 * @default "delete"
	 */
	cleanupPolicy?: "delete" | "compact" | "compact,delete" | undefined;
}

/**
 * A topic with its defaults filled in, as `defineTopics` returns it
 */
export interface Topic<Schema extends ZodType = ZodType> {
	readonly name: string;
	readonly schema: Schema;
	readonly key: (event: z.output<Schema>) => string;
	readonly partitions: number | undefined;
	readonly replicationFactor: number | undefined;
	readonly retentionMs: number;
	readonly cleanupPolicy: "delete" | "compact" | "compact,delete";
}

export type Topics<Schemas extends Record<string, ZodType>> = {
	readonly [Name in keyof Schemas]: Topic<Schemas[Name]>;
};

/**
 * Any set of topics, as `createKafka` takes them
 */
export type AnyTopics = Record<string, Topic<any>>;

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
export declare function defineTopics<Schemas extends Record<string, ZodType>>(
	topics: { [Name in keyof Schemas]: TopicOptions<Schemas[Name]> },
): Topics<Schemas>;
