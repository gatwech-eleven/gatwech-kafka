import type { ConnectionOptions } from "node:tls";
import type { Logger } from "@gatwech/core/env";
import type { z } from "@gatwech/utils/zod";
import type { SASLOptions } from "kafkajs";
import type { KAFKA_ERROR_CODES } from "../error-codes";
import type { AnyTopics, Topic } from "../topics/types";

export interface KafkaOptions {
	/**
	 * Names this service to the broker and in its logs
	 *
	 * @example "payvo-auth-service"
	 */
	clientId: string;
	/**
	 * The brokers to connect to, as `host:port`
	 *
	 * @example ["localhost:9092"]
	 */
	brokers: string[];
	/**
	 * The topics this service publishes and consumes
	 * @description Declared once with `defineTopics`, the way a plugin carries
	 * its schema. `publish` and `consume` name them by key, `provision`
	 * creates them, and `kafka.topics` holds them as the broker knows them
	 *
	 * @default {}
	 */
	topics?: AnyTopics | undefined;
	/**
	 * Turns the client on or off
	 * @description Off, `connect`, `provision`, `disconnect` and `consume` do
	 * nothing, and `publish` logs at `warn` and sends nothing. It replaces
	 * wiring a service's `KAFKA_ENABLED` into four options and an `if` in each
	 * of its start and stop functions
	 *
	 * @default true
	 */
	enabled?: boolean | undefined;
	/**
	 * Prefixes the name of every topic, and of its DLQ
	 * @description Keeps environments that share a broker apart, without the
	 * topic declarations reading the environment. It replaces the `topic()`
	 * helper each service writes for itself. A topic opts out with
	 * `namespaced: false`
	 *
	 * @example "prod" makes `user.registered` `prod.user.registered`
	 * @default undefined, topics keep their declared names
	 */
	namespace?: string | undefined;
	/**
	 * Where this client logs
	 * @description The same logger options as `payvoAuth`. Nothing is read from
	 * the environment
	 */
	logger?: Logger | undefined;
	/**
	 * Encrypts the connection to the brokers
	 *
	 * @default false
	 */
	ssl?: boolean | ConnectionOptions | undefined;
	/**
	 * Signs in to the brokers
	 *
	 * @default undefined, no sign-in
	 */
	sasl?: SASLOptions | undefined;
	/**
	 * Compresses what is published
	 *
	 * @default "gzip"
	 */
	compression?: "gzip" | "none" | undefined;
	/**
	 * Counts publishes, failures and reconnections
	 *
	 * @default true
	 */
	metrics?: boolean | undefined;
	connect?:
		| {
				/**
				 * How many times `connect()` tries before it throws
				 * `BROKER_UNREACHABLE`, a DNS failure included
				 *
				 * @default 5
				 */
				maxAttempts?: number | undefined;
				/**
				 * How long to wait between tries, in milliseconds
				 *
				 * @default 2_000
				 */
				retryDelayMs?: number | undefined;
		  }
		| undefined;
	/**
	 * What `provision()` uses for a topic that sets neither
	 * @description A topic's DLQ gets one partition, the topic's replication
	 * factor and retention, and the `"delete"` cleanup policy
	 */
	provisioning?:
		| {
				/**
				 * How many partitions a new topic has
				 *
				 * @default 3
				 */
				partitions?: number | undefined;
				/**
				 * How many brokers keep a copy of each partition
				 *
				 * @default 1
				 */
				replicationFactor?: number | undefined;
				/**
				 * How long to wait for each partition's leader, in milliseconds
				 *
				 * @default 15_000
				 */
				waitForLeadersMs?: number | undefined;
		  }
		| undefined;
	circuitBreaker?:
		| {
				/**
				 * Stops publishing while the brokers are failing, instead of piling
				 * up requests
				 *
				 * @default true
				 */
				enabled?: boolean | undefined;
				/**
				 * How many publishes in a row fail before the circuit opens
				 *
				 * @default 5
				 */
				failureThreshold?: number | undefined;
				/**
				 * How long the circuit stays open before one publish is let through,
				 * in milliseconds. If it works the circuit closes
				 *
				 * @default 30_000
				 */
				resetTimeoutMs?: number | undefined;
		  }
		| undefined;
	reconnection?:
		| {
				/**
				 * Reconnects when the connection drops
				 * @description It also starts when the first `connect()` fails, so a
				 * service can carry on without Kafka and be connected later. After
				 * each reconnection the topics are provisioned again
				 *
				 * @default true
				 */
				enabled?: boolean | undefined;
				/**
				 * How often to check the connection, in milliseconds
				 *
				 * @default 60_000
				 */
				intervalMs?: number | undefined;
		  }
		| undefined;
}

/**
 * The topics of a client, by key
 */
export type InferTopics<Options extends KafkaOptions> = Options extends {
	topics: infer T extends AnyTopics;
}
	? T
	: {};

/**
 * One of the topics of a client, by key
 */
export type InferTopic<
	Options extends KafkaOptions,
	Name extends keyof InferTopics<Options>,
> = InferTopics<Options>[Name] extends infer T extends Topic<any> ? T : never;

export type Kafka<Options extends KafkaOptions = KafkaOptions> = {
	/**
	 * Connects the producer, then provisions the topics. Throws
	 * `BROKER_UNREACHABLE` once `connect.maxAttempts` is spent
	 */
	connect: () => Promise<void>;
	/**
	 * Disconnects the producer and every consumer this client created
	 * @description It never throws: a consumer that fails to leave its group is
	 * logged, and the rest are still disconnected, so a shutdown carries on
	 */
	disconnect: () => Promise<void>;
	healthCheck: () => Promise<{
		/**
		 * Whether a broker answered. Checking never connects the client, and a
		 * client that is not `enabled` is never healthy
		 */
		healthy: boolean;
		/**
		 * Whether `connect()` has succeeded and `disconnect()` has not been called
		 */
		connected: boolean;
		/**
		 * Whether publishes are going through: `"open"` refuses them, and
		 * `"half-open"` lets one through to find out
		 */
		circuit: "closed" | "open" | "half-open";
	}>;
	/**
	 * Creates the topics that are missing, and a DLQ for each, so nothing has to
	 * be created on first use
	 * @description `connect()` already calls it
	 */
	provision: () => Promise<void>;
	/**
	 * Publishes one event to one of the topics
	 *
	 * @example
	 * await kafka.publish({
	 * 	topic: "userRegistered",
	 * 	event: { userId, email },
	 * });
	 */
	publish: <Name extends keyof InferTopics<Options> & string>(options: {
		/**
		 * The key of the topic in the `topics` option of `createKafka`
		 * @description The topic's schema checks the event, and its `key` picks the
		 * partition
		 *
		 * @example "userRegistered"
		 */
		topic: Name;
		/**
		 * The event to publish
		 * @description The publish is refused with `EVENT_REJECTED_BY_SCHEMA` when
		 * the topic's schema rejects it
		 */
		event: z.input<InferTopic<Options, Name>["schema"]>;
		/**
		 * Ties this message to the others of one request
		 * @description Consumers get it as `ctx.correlationId`. The message has the
		 * shape `createEventPublisher` writes today, so consumers that have not
		 * moved read it unchanged: the body is the event with
		 * `metadata: { source, correlationId }` added, `source` is the `clientId`,
		 * and the headers are `event-version`, `event-type`, `timestamp` and
		 * `correlation-id`
		 *
		 * @default a new id for each publish
		 */
		correlationId?: string | undefined;
		/**
		 * Extra headers on the message
		 */
		headers?: Record<string, string> | undefined;
	}) => Promise<void>;
	/**
	 * Publishes several events to one of the topics in one request
	 */
	publishBatch: <Name extends keyof InferTopics<Options> & string>(options: {
		/**
		 * The key of the topic in the `topics` option of `createKafka`
		 *
		 * @example "userRegistered"
		 */
		topic: Name;
		/**
		 * The events to publish
		 * @description Each one is checked first, and none is sent if any is
		 * rejected
		 */
		events: z.input<InferTopic<Options, Name>["schema"]>[];
		/**
		 * Ties these messages to the others of one request
		 *
		 * @default a new id for each publish
		 */
		correlationId?: string | undefined;
		/**
		 * Extra headers on every message
		 */
		headers?: Record<string, string> | undefined;
	}) => Promise<void>;
	/**
	 * Reads one of the topics, with retries and a DLQ
	 *
	 * @example
	 * await kafka.consume({
	 * 	groupId: "payvo-wallet-service.user-registered",
	 * 	topic: "userRegistered",
	 * 	handler: async ({ event }) => {
	 * 		await createWallet(event.userId);
	 * 	},
	 * });
	 */
	consume: <Name extends keyof InferTopics<Options> & string>(options: {
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
		handler: (ctx: {
			/**
			 * The event, parsed by the topic's schema
			 */
			event: z.output<InferTopic<Options, Name>["schema"]>;
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
		}) => Promise<void>;
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
		 * @description `false` starts at the end of the topic, so messages
		 * published before the group joined are skipped
		 *
		 * @default false
		 */
		fromBeginning?: boolean | undefined;
	}) => Promise<{
		groupId: string;
		/**
		 * The partitions this consumer paused after a failed publish to the DLQ
		 */
		paused: () => { topic: string; partitions: number[] }[];
		/**
		 * Resumes the paused partitions. The message that was kept is handled
		 * again
		 */
		resume: () => void;
		/**
		 * Leaves the group and stops reading
		 */
		disconnect: () => Promise<void>;
	}>;
	/**
	 * The topics as the broker knows them: with the `namespace` in their names
	 */
	topics: InferTopics<Options>;
	options: Options;
	$ERROR_CODES: typeof KAFKA_ERROR_CODES;
};
