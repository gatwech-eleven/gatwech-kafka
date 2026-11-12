import type { ConnectionOptions } from "node:tls";
import type { Logger } from "@gatwech/core/env";
import type { SASLOptions } from "kafkajs";
import type { KAFKA_ERROR_CODES } from "../error-codes";
import type { ConsumeOptions, KafkaConsumer } from "../consumer/types";
import type { PublishBatchOptions, PublishOptions } from "../publisher/types";
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

export interface KafkaHealth {
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
}

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
	healthCheck: () => Promise<KafkaHealth>;
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
	publish: <Name extends keyof InferTopics<Options> & string>(
		options: PublishOptions<Name, InferTopic<Options, Name>>,
	) => Promise<void>;
	/**
	 * Publishes several events to one of the topics in one request
	 */
	publishBatch: <Name extends keyof InferTopics<Options> & string>(
		options: PublishBatchOptions<Name, InferTopic<Options, Name>>,
	) => Promise<void>;
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
	consume: <Name extends keyof InferTopics<Options> & string>(
		options: ConsumeOptions<InferTopic<Options, Name>, Name>,
	) => Promise<KafkaConsumer>;
	/**
	 * The topics as the broker knows them: with the `namespace` in their names
	 */
	topics: InferTopics<Options>;
	options: Options;
	$ERROR_CODES: typeof KAFKA_ERROR_CODES;
};
