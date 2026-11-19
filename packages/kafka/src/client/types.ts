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
	 * @example "example-service"
	 */
	clientId: string;
	/**
	 * The brokers to connect to, as `host:port`
	 *
	 * @example ["localhost:9092"]
	 */
	brokers: string[];
	/**
	 * The topics this service publishes and consumes, declared once with
	 * `defineTopics`. `publish` and `consume` name them by key, `provision`
	 * creates them, and `kafka.topics` holds them as the broker knows them
	 *
	 * @default {}
	 */
	topics?: AnyTopics | undefined;
	/**
	 * Turns the client on or off. Off, `connect`, `provision`, `disconnect` and
	 * `consume` do nothing, and `publish` logs at `warn` and sends nothing
	 *
	 * @default true
	 */
	enabled?: boolean | undefined;
	/**
	 * Prefixes the name of every topic, and of its DLQ. It keeps environments
	 * that share a broker apart, without the topic declarations reading the
	 * environment. A topic opts out with `namespaced: false`
	 *
	 * @example "prod" makes `example.created` `prod.example.created`
	 * @default undefined, topics keep their declared names
	 */
	namespace?: string | undefined;
	/**
	 * Where this client logs: a `Logger` from `@gatwech/core/env`. Nothing is
	 * read from the environment
	 *
	 * @example { level: "info", tag: "example-service" }
	 * @default its lines are tagged "Payvo Kafka"
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
	connect?:
		| {
				/**
				 * How many times a connection is tried before it throws
				 * `FAILED_TO_CONNECT`, a DNS failure included
				 *
				 * @default 5 tries
				 */
				attempts?: number | undefined;
				/**
				 * How long to wait between tries, in milliseconds
				 *
				 * @default 2_000 milliseconds
				 */
				retryDelayMs?: number | undefined;
		  }
		| undefined;
	/**
	 * What `provision()` uses for a topic that sets neither. A topic's DLQ gets
	 * one partition, the topic's replication factor and retention, and the
	 * `"delete"` cleanup policy
	 */
	provisioning?:
		| {
				/**
				 * How many partitions a new topic has
				 *
				 * @default 3 partitions
				 */
				partitions?: number | undefined;
				/**
				 * How many brokers keep a copy of each partition
				 *
				 * @default 1 broker
				 */
				replicationFactor?: number | undefined;
				/**
				 * How long to wait for each partition's leader, in milliseconds
				 *
				 * @default 15_000 milliseconds
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
				 * @default 5 publishes
				 */
				failureThreshold?: number | undefined;
				/**
				 * How long the circuit stays open before one publish is let through,
				 * in milliseconds. If it works the circuit closes
				 *
				 * @default 30_000 milliseconds
				 */
				resetTimeoutMs?: number | undefined;
		  }
		| undefined;
	reconnection?:
		| {
				/**
				 * Reconnects when the connection drops. It also starts when the first
				 * connection fails, so a service can carry on without Kafka and be
				 * connected later. After each reconnection the topics are provisioned
				 * again, and the consumers waiting for it join
				 *
				 * @default true
				 */
				enabled?: boolean | undefined;
				/**
				 * How often to check the connection, in milliseconds
				 *
				 * @default 60_000 milliseconds
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
	 * `FAILED_TO_CONNECT` once `connect.attempts` is spent. Optional: every
	 * operation connects the client first when it isn't, so call it at start to
	 * connect before the first operation needs it. Creating the client never
	 * connects
	 */
	connect: () => Promise<void>;
	/**
	 * Disconnects the producer and every consumer this client created. It never
	 * throws: a consumer that fails to leave its group is logged, and the rest
	 * are still disconnected, so a shutdown carries on
	 */
	disconnect: () => Promise<void>;
	healthCheck: () => Promise<{
		/**
		 * Whether a broker answered. Checking never connects the client, and a
		 * client that is not `enabled` is never healthy
		 */
		healthy: boolean;
		/**
		 * Whether a connection has succeeded and `disconnect()` has not been
		 * called since
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
	 * be created on first use. Connecting already does it, so a client that
	 * isn't connected is connected instead
	 */
	provision: () => Promise<void>;
	/**
	 * Publishes one event to one of the topics. It connects the client first
	 * when it isn't, and throws `FAILED_TO_CONNECT` when no broker answers.
	 * Refused with `CIRCUIT_OPEN` while publishes are failing, with
	 * `TOPIC_NOT_PROVISIONED` when the topic is gone (a publish never creates
	 * one, and it waits for kafkajs to retry first, about 10 seconds) and with
	 * `FAILED_TO_PUBLISH` when the broker refuses the message. A client that is
	 * not `enabled` logs at `warn` and sends nothing
	 *
	 * @example
	 * await kafka.publish({
	 * 	topic: "example",
	 * 	event: { id, label },
	 * });
	 */
	publish: <Name extends keyof InferTopics<Options> & string>(options: {
		/**
		 * The key of the topic in the `topics` option of `createKafka`. The
		 * topic's schema checks the event, and its `key` picks the partition
		 *
		 * @example "example"
		 */
		topic: Name;
		/**
		 * The event to publish. The publish is refused with `INVALID_EVENT` when
		 * the topic's schema rejects it
		 */
		event: z.input<InferTopic<Options, Name>["schema"]>;
		/**
		 * Ties this message to the others of one request: consumers get it as
		 * `ctx.correlationId`. The body is the event with
		 * `metadata: { source, correlationId }` added, `source` is the
		 * `clientId`, and the headers are `event-version`, `event-type`,
		 * `timestamp` and `correlation-id`
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
		 * @example "example"
		 */
		topic: Name;
		/**
		 * The events to publish. Each one is checked first, and none is sent if
		 * any is rejected
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
	 * Reads one of the topics, with retries and a DLQ, and resolves once the
	 * consumer has joined its group. Before the client is connected it waits,
	 * and joins once a connection, made by any operation or by a reconnection,
	 * has provisioned the topics, since the DLQ is published with the client's
	 * producer. So it can be called at start, before anyone knows whether the
	 * brokers are up. When `disconnect()` comes first, it resolves with a
	 * consumer that does nothing.
	 *
	 * Refused at once with `TOPIC_NOT_DECLARED`, with `INVALID_RETRY` and
	 * `INVALID_HEARTBEAT_INTERVAL` for settings that would get the consumer
	 * removed from its group, and with `FAILED_TO_START_CONSUMER` when it can't
	 * join or subscribe. `disconnect()` of the client disconnects every consumer
	 * it created. A client that is not `enabled` logs at `warn` and returns a
	 * consumer that does nothing
	 *
	 * @example
	 * await kafka.consume({
	 * 	groupId: "example-service.example",
	 * 	topic: "example",
	 * 	handler: async ({ event }) => {
	 * 		await store(event);
	 * 	},
	 * });
	 */
	consume: <Name extends keyof InferTopics<Options> & string>(options: {
		/**
		 * The consumer group. The consumers of one group share the topic's
		 * partitions. One group reads one topic: a service that reads several
		 * calls `consume` once for each, and each gets its own group. A group
		 * that has no committed offset starts where `fromBeginning` says, so
		 * naming a group that exists keeps its place
		 *
		 * @example "example-service.example"
		 * @default `${clientId}.${topic}`, e.g. "example-service.example"
		 */
		groupId?: string | undefined;
		/**
		 * The key of the topic to read in the `topics` option of `createKafka`
		 *
		 * @example "example"
		 */
		topic: Name;
		/**
		 * Handles one event. Let errors throw: when it throws, it is called again
		 * after `retry.backoffMs`, up to `retry.attempts` times in all, and every
		 * failure is logged. Then the message goes to the DLQ. A handler that
		 * catches its own error to log it hides the message from all of that,
		 * and its offset is committed
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
					 * @default 3 calls
					 */
					attempts?: number | undefined;
					/**
					 * How long to wait after the first failure, in milliseconds. It
					 * doubles after each one, up to 30 seconds. The total must stay
					 * below `session.timeoutMs`
					 *
					 * @default 500 milliseconds
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
					 * How many times a failed publish to the DLQ is tried. Then the
					 * partition is paused, an `error` is logged and the offset is left
					 * uncommitted: the message is neither lost nor handled again until
					 * `resume()`
					 *
					 * @default 3 tries
					 */
					attempts?: number | undefined;
			  }
			| undefined;
		session?:
			| {
					/**
					 * How long the broker waits for a heartbeat before it removes the
					 * consumer from its group, in milliseconds
					 *
					 * @default 30_000 milliseconds
					 */
					timeoutMs?: number | undefined;
					/**
					 * How often the consumer tells the broker it is alive, in
					 * milliseconds. Shorter than `timeoutMs`, usually a third of it
					 *
					 * @default 3_000 milliseconds
					 */
					heartbeatIntervalMs?: number | undefined;
					/**
					 * How long the group waits for each member to rejoin when it
					 * rebalances, in milliseconds
					 *
					 * @default 60_000 milliseconds
					 */
					rebalanceTimeoutMs?: number | undefined;
			  }
			| undefined;
		/**
		 * Where a group with no committed offset starts. `false` starts at the end
		 * of the topic, so messages published before the group joined are skipped
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
	/**
	 * Share types: the event of each topic, by its key, as a handler receives
	 * it (what the topic's schema outputs). It holds nothing at runtime
	 *
	 * @example
	 * type Example = typeof kafka.$Infer.example;
	 */
	$Infer: {
		[Name in keyof InferTopics<Options>]: z.output<
			InferTopic<Options, Name>["schema"]
		>;
	};
};
