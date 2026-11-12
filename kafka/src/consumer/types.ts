import type { z } from "@gatwech/utils/zod";
import type { Topic } from "../topics/types";

export interface ConsumeContext<T extends Topic<any> = Topic> {
	/**
	 * The event, parsed by the topic's schema
	 */
	event: z.output<T["schema"]>;
	/**
	 * The topic's name on the broker, its namespace included
	 */
	topic: string;
	partition: number;
	offset: string;
	/**
	 * The partition key the message was published with
	 */
	key: string | null;
	headers: Record<string, string>;
	/**
	 * What the publisher passed as `correlationId`
	 */
	correlationId: string | undefined;
	/**
	 * The `clientId` of the service that published the message
	 */
	source: string | undefined;
}

export interface ConsumeOptions<
	T extends Topic<any> = Topic,
	Name extends string = string,
> {
	/**
	 * The consumer group. The consumers of one group share the topic's
	 * partitions
	 * @description One group reads one topic. A service that reads several
	 * calls `consume` once for each, and each gets its own group. A group that
	 * has no committed offset starts where `fromBeginning` says, so naming a
	 * group that exists keeps its place
	 *
	 * @example "payvo-wallet-service.user-registered"
	 * @default `${clientId}.${topic}`, e.g. "payvo-wallet-service.userRegistered"
	 */
	groupId?: string | undefined;
	/**
	 * The key of the topic to read in the `topics` option of `createKafka`
	 *
	 * @example "userRegistered"
	 */
	topic: Name;
	/**
	 * Handles one event
	 * @description Let errors throw. When it throws, it is called again after
	 * `retry.backoffMs`, up to `retry.attempts` times in all, and every failure
	 * is logged. Then the message goes to the DLQ. A handler that catches its
	 * own error to log it hides the message from all of that, and its offset is
	 * committed
	 */
	handler: (ctx: ConsumeContext<T>) => Promise<void>;
	retry?:
		| {
				/**
				 * How many times the handler is called for one message, the first
				 * call included
				 *
				 * @default 3
				 */
				attempts?: number | undefined;
				/**
				 * How long to wait after the first failure, in milliseconds
				 * @description It doubles after each one, up to 30 seconds. The
				 * total must stay below `session.timeoutMs`
				 *
				 * @default 500
				 */
				backoffMs?: number | undefined;
		  }
		| undefined;
	dlq?:
		| {
				/**
				 * Where a message goes once the handler has failed every attempt, or
				 * when it doesn't match the topic's schema. It must exist:
				 * `provision()` creates the default one of every topic
				 *
				 * @default the topic's name with `.dlq` added, its namespace included
				 */
				topic?: string | undefined;
				/**
				 * How many times a failed publish to the DLQ is tried
				 * @description Then the partition is paused, an `error` is logged and
				 * the offset is left uncommitted: the message is neither lost nor
				 * handled again until `resume()`
				 *
				 * @default 3
				 */
				publishAttempts?: number | undefined;
		  }
		| undefined;
	session?:
		| {
				/**
				 * How long the broker waits for a heartbeat before it removes the
				 * consumer from its group, in milliseconds
				 *
				 * @default 30_000
				 */
				timeoutMs?: number | undefined;
				/**
				 * How often the consumer tells the broker it is alive, in
				 * milliseconds. Shorter than `timeoutMs`, usually a third of it
				 *
				 * @default 3_000
				 */
				heartbeatIntervalMs?: number | undefined;
				/**
				 * How long the group waits for each member to rejoin when it
				 * rebalances, in milliseconds
				 *
				 * @default 60_000
				 */
				rebalanceTimeoutMs?: number | undefined;
		  }
		| undefined;
	/**
	 * Where a group with no committed offset starts
	 * @description `false` starts at the end of the topic, so messages published
	 * before the group joined are skipped
	 *
	 * @default false
	 */
	fromBeginning?: boolean | undefined;
}

export interface KafkaConsumer {
	groupId: string;
	/**
	 * The partitions this consumer paused after a failed publish to the DLQ
	 */
	paused: () => { topic: string; partitions: number[] }[];
	/**
	 * Resumes the paused partitions. The message that was kept is handled again
	 */
	resume: () => void;
	/**
	 * Leaves the group and stops reading
	 */
	disconnect: () => Promise<void>;
}
