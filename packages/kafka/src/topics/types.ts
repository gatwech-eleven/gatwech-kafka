import type { z, ZodType } from "@gatwech/utils/zod";

export interface TopicOptions<Schema extends ZodType = ZodType> {
	/**
	 * The topic's name on the broker
	 *
	 * @example "example.created"
	 */
	name: string;
	/**
	 * Whether the client's `namespace` is put in front of the name. `false`
	 * keeps the name as declared, for a topic that other services already read
	 * under that name: with the namespace, its name on the broker changes
	 *
	 * @default true
	 */
	namespaced?: boolean | undefined;
	/**
	 * What an event on this topic looks like. Publishers refuse an event it
	 * rejects, and consumers send a message it rejects to the DLQ
	 */
	schema: Schema;
	/**
	 * The partition key of an event. Events with the same key sit on one
	 * partition, so they are read in the order they were published
	 *
	 * @example (event) => event.id
	 */
	key: (event: z.output<Schema>) => string;
	/**
	 * How many partitions the topic has. More partitions let more consumers of
	 * a group read at once
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
	 * How long a message is kept, in milliseconds. Ignored when `cleanupPolicy`
	 * is `"compact"`
	 *
	 * @default 604_800_000 milliseconds, 7 days
	 */
	retentionMs?: number | undefined;
	/**
	 * What the broker does with old messages. `"delete"` drops them after
	 * `retentionMs`, and `"compact"` keeps the latest message of each key
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
	readonly namespaced: boolean;
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
