import type { ConnectionOptions } from "node:tls";
import type { Logger } from "@gatwech/core/env";
import type { SASLOptions } from "kafkajs";
import type { KAFKA_ERROR_CODES } from "../error-codes";
import type { ConsumeOptions, KafkaConsumer } from "../consumer/types";
import type { Publisher, PublisherOptions } from "../publisher/types";
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
	 * its schema. `publisher` and `consume` name them by key, `provision`
	 * creates them, and `kafka.topics` holds them as the broker knows them
	 *
	 * @default {}
	 */
	topics?: AnyTopics | undefined;
	/**
	 * Prefixes the name of every topic, and of its DLQ
	 * @description Keeps environments that share a broker apart, without the
	 * topic declarations reading the environment. It replaces the `topic()`
	 * helper each service writes for itself
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
	 */
	provisioning?:
		| {
				/**
				 * @default 1
				 */
				partitions?: number | undefined;
				/**
				 * @default 1
				 */
				replicationFactor?: number | undefined;
				/**
				 * How long to wait for each partition's leader, in milliseconds
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
				/**
				 * Called after each reconnection, e.g. to start the consumers again
				 */
				onReconnect?: (() => Promise<void>) | undefined;
		  }
		| undefined;
}

/**
 * The topics of a client, by key
 */
export type TopicsOf<Options extends KafkaOptions> = Options extends {
	topics: infer T extends AnyTopics;
}
	? T
	: {};

/**
 * One of the topics of a client, by key
 */
export type TopicOf<
	Options extends KafkaOptions,
	Name extends keyof TopicsOf<Options>,
> = TopicsOf<Options>[Name] extends infer T extends Topic<any> ? T : never;

export interface KafkaHealth {
	/**
	 * Whether a broker answered. Checking never connects the client
	 */
	healthy: boolean;
	/**
	 * Whether `connect()` has succeeded and `disconnect()` has not been called
	 */
	connected: boolean;
	circuit: "closed" | "open" | "half-open";
}

export type Kafka<Options extends KafkaOptions = KafkaOptions> = {
	/**
	 * Connects the producer. Throws `BROKER_UNREACHABLE` once
	 * `connect.maxAttempts` is spent
	 */
	connect: () => Promise<void>;
	/**
	 * Disconnects the producer and every consumer this client created
	 */
	disconnect: () => Promise<void>;
	healthCheck: () => Promise<KafkaHealth>;
	/**
	 * Creates the topics that are missing, and a DLQ for each, so nothing has to
	 * be created on first use
	 */
	provision: () => Promise<void>;
	/**
	 * A publisher for one of the topics
	 *
	 * @example
	 * const userRegistered = kafka.publisher({
	 * 	topic: "userRegistered",
	 * 	source: "payvo-auth-service",
	 * });
	 */
	publisher: <Name extends keyof TopicsOf<Options> & string>(
		options: PublisherOptions<Name>,
	) => Publisher<TopicOf<Options, Name>>;
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
	consume: <Name extends keyof TopicsOf<Options> & string>(
		options: ConsumeOptions<TopicOf<Options, Name>, Name>,
	) => Promise<KafkaConsumer>;
	/**
	 * The topics as the broker knows them: with the `namespace` in their names
	 */
	topics: TopicsOf<Options>;
	options: Options;
	$ERROR_CODES: typeof KAFKA_ERROR_CODES;
};

/**
 * Creates the Kafka client of a service. One per service
 *
 * @example
 * export const kafka = createKafka({
 * 	clientId: "payvo-auth-service",
 * 	brokers: ["localhost:9092"],
 * 	topics,
 * 	namespace: env.KAFKA_NAMESPACE,
 * });
 */
export declare function createKafka<Options extends KafkaOptions>(
	options: Options & Record<never, never>,
): Kafka<Options>;
