import { createLogger, type InternalLogger } from "@gatwech/core/env";
import { z } from "@gatwech/utils/zod";
import {
	CompressionTypes,
	Kafka as KafkaJS,
	KafkaJSNumberOfRetriesExceeded,
	KafkaJSProtocolError,
	type LogEntry,
	logLevel,
	Partitioners,
} from "kafkajs";
import { initializeKafkaTopics } from "../config/initialize.topics";
import type { TopicConfig } from "../config/topics/topic-config-schema";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import { defaultDlqTopicName, resolveTopics } from "../topics";
import { buildMessages } from "./build-messages";
import { createCircuitBreaker } from "./circuit-breaker";
import { createReconnectionMonitor } from "./reconnection";
import type { Kafka, KafkaOptions } from "./types";

export type * from "./types";

const sleep = (ms: number) =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

const messageOf = (error: unknown) =>
	error instanceof Error ? error.message : String(error);

// kafkajs wraps an error it retried in KafkaJSNumberOfRetriesExceeded
const isMissingTopic = (error: unknown): boolean =>
	error instanceof KafkaJSProtocolError
		? error.type === "UNKNOWN_TOPIC_OR_PARTITION"
		: error instanceof KafkaJSNumberOfRetriesExceeded &&
			isMissingTopic(error.cause);

/**
 * Sends what kafkajs warns about and fails on to the client's logger
 */
const createLogCreator =
	(logger: InternalLogger) =>
	() =>
	({ level, log }: LogEntry) => {
		const { message, ...details } = log;
		if (level === logLevel.ERROR) {
			logger.error(message, details);
		} else if (level === logLevel.WARN) {
			logger.warn(message, details);
		}
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
export function createKafka<Options extends KafkaOptions>(
	options: Options & Record<never, never>,
): Kafka<Options> {
	if (!options.brokers || options.brokers.length === 0) {
		throw new KafkaError(KAFKA_ERROR_CODES.BROKERS_REQUIRED);
	}

	const logger = createLogger(options.logger);
	const isEnabled = options.enabled ?? true;
	const topics = resolveTopics(options.topics ?? {}, options.namespace);
	const attempts = Math.max(1, options.connect?.attempts ?? 5);
	const retryDelayMs = options.connect?.retryDelayMs ?? 2_000;

	const brokerOptions = {
		clientId: options.clientId,
		brokers: options.brokers,
		ssl: options.ssl,
		sasl: options.sasl,
	};
	// kafkajs retries a connection by itself, which would multiply
	// `connect.attempts`. The attempts are made on a client that doesn't
	const connectionClient = new KafkaJS({
		...brokerOptions,
		logLevel: logLevel.NOTHING,
		retry: { retries: 0 },
	});
	const client = new KafkaJS({
		...brokerOptions,
		logLevel: logLevel.WARN,
		logCreator: createLogCreator(logger),
	});
	let admin = connectionClient.admin();
	// Topics are provisioned, so a send never creates one (#23)
	const createProducer = () =>
		client.producer({
			allowAutoTopicCreation: false,
			idempotent: true,
			createPartitioner: Partitioners.DefaultPartitioner,
		});
	let producer = createProducer();

	const breaker = createCircuitBreaker({
		enabled: options.circuitBreaker?.enabled ?? true,
		failureThreshold: options.circuitBreaker?.failureThreshold ?? 5,
		resetTimeoutMs: options.circuitBreaker?.resetTimeoutMs ?? 30_000,
	});

	// Each topic and the DLQ its messages go to, as the broker knows them
	const provisioned: Record<string, TopicConfig> = {};
	for (const [key, topic] of Object.entries(options.topics ?? {})) {
		provisioned[key] = topics[key] as TopicConfig;
		provisioned[`${key}.dlq`] = {
			name: defaultDlqTopicName(topic, options.namespace),
			schema: z.unknown(),
			partitions: 1,
			replicationFactor: topic.replicationFactor,
			retentionMs: topic.retentionMs,
			cleanupPolicy: "delete",
		};
	}

	let isConnected = false;
	let isClosed = false;
	let connecting: Promise<void> | undefined;

	// A kafkajs client can't be used again once it has disconnected: its next
	// request fails with "write after end". A late event from a client that was
	// replaced says nothing about the one in use
	const watchClients = () => {
		const watchedProducer = producer;
		const watchedAdmin = admin;
		watchedProducer.on(watchedProducer.events.DISCONNECT, () => {
			if (producer === watchedProducer) {
				isConnected = false;
			}
		});
		watchedAdmin.on(watchedAdmin.events.DISCONNECT, () => {
			if (admin === watchedAdmin) {
				isConnected = false;
			}
		});
	};
	watchClients();

	const closeClients = async () => {
		const disconnections = await Promise.allSettled([
			producer.disconnect(),
			admin.disconnect(),
		]);
		isConnected = false;
		admin = connectionClient.admin();
		producer = createProducer();
		watchClients();
		return disconnections;
	};

	const connectWithAttempts = async () => {
		let lastError: unknown;
		for (let attempt = 1; attempt <= attempts && !isClosed; attempt++) {
			try {
				await admin.connect();
				await producer.connect();
				isConnected = true;
				logger.info("Connected to Kafka", { attempt });
				return;
			} catch (error) {
				lastError = error;
				await closeClients();
				logger.warn("Could not connect to Kafka", {
					attempt,
					attempts,
					error: messageOf(error),
				});
				if (attempt < attempts) {
					await sleep(retryDelayMs);
				}
			}
		}
		if (!isClosed) {
			throw new KafkaError(KAFKA_ERROR_CODES.BROKER_UNREACHABLE, lastError);
		}
	};

	const provision: Kafka<Options>["provision"] = async () => {
		if (!isEnabled || Object.keys(provisioned).length === 0) {
			return;
		}
		if (!isConnected) {
			throw new KafkaError(KAFKA_ERROR_CODES.NOT_CONNECTED);
		}
		await initializeKafkaTopics(
			admin,
			{
				topics: provisioned,
				partitions: options.provisioning?.partitions,
				replicationFactor: options.provisioning?.replicationFactor,
				waitForLeadersMs: options.provisioning?.waitForLeadersMs,
			},
			false,
		);
	};

	const monitor = createReconnectionMonitor({
		intervalMs: options.reconnection?.intervalMs ?? 60_000,
		check: async () => {
			if (!isConnected) {
				return false;
			}
			try {
				await admin.describeCluster();
				return true;
			} catch {
				return false;
			}
		},
		reconnect: async () => {
			if (isClosed) {
				return;
			}
			if (connecting) {
				await connecting;
				return;
			}
			logger.info("Reconnecting to Kafka");
			await closeClients();
			await start();
		},
		onError: (error) => {
			logger.warn("Could not reconnect to Kafka", { error: messageOf(error) });
		},
	});

	const connectAndProvision = async () => {
		try {
			await connectWithAttempts();
			if (!isClosed) {
				await provision();
			}
		} catch (error) {
			await closeClients();
			throw error;
		} finally {
			if (!isClosed && (options.reconnection?.enabled ?? true)) {
				monitor.start();
			}
		}
	};

	const start = () => {
		connecting ??= connectAndProvision().finally(() => {
			connecting = undefined;
		});
		return connecting;
	};

	const connect: Kafka<Options>["connect"] = async () => {
		if (!isEnabled) {
			logger.info("Kafka is disabled; not connecting");
			return;
		}
		isClosed = false;
		if (!isConnected) {
			await start();
		}
	};

	const disconnect: Kafka<Options>["disconnect"] = async () => {
		isClosed = true;
		monitor.stop();
		if (!isEnabled) {
			return;
		}
		const disconnections = await closeClients();
		for (const disconnection of disconnections) {
			if (disconnection.status === "rejected") {
				logger.error("Could not disconnect from Kafka", {
					error: messageOf(disconnection.reason),
				});
			}
		}
	};

	const healthCheck: Kafka<Options>["healthCheck"] = async () => {
		const circuit = breaker.state();
		if (!isEnabled || !isConnected) {
			return { healthy: false, connected: false, circuit };
		}
		try {
			const { controller } = await admin.describeCluster();
			// A controller's node id can be 0
			return { healthy: controller !== null, connected: isConnected, circuit };
		} catch (error) {
			logger.debug("Kafka health check failed", { error: messageOf(error) });
			return { healthy: false, connected: isConnected, circuit };
		}
	};

	const send = async (message: {
		topic: string;
		events: unknown[];
		correlationId?: string | undefined;
		headers?: Record<string, string> | undefined;
	}) => {
		if (!isEnabled) {
			logger.warn("Kafka is disabled; not publishing", { topic: message.topic });
			return;
		}
		const topic = topics[message.topic];
		if (!topic) {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_NOT_DECLARED, message.topic);
		}
		if (!isConnected) {
			throw new KafkaError(KAFKA_ERROR_CODES.NOT_CONNECTED);
		}

		const messages = buildMessages({
			topic,
			clientId: options.clientId,
			events: message.events,
			correlationId: message.correlationId,
			headers: message.headers,
		});
		let missingTopic: unknown;
		try {
			await breaker.run(async () => {
				try {
					await producer.send({
						topic: topic.name,
						messages,
						compression:
							options.compression === "none"
								? CompressionTypes.None
								: CompressionTypes.GZIP,
					});
				} catch (error) {
					// A topic that's gone is not the broker failing, so it doesn't count
					// against the circuit, which would refuse every other topic too
					if (isMissingTopic(error)) {
						missingTopic = error;
						return;
					}
					throw error;
				}
			});
		} catch (error) {
			if (error instanceof KafkaError) {
				throw error;
			}
			throw new KafkaError(KAFKA_ERROR_CODES.PUBLISH_FAILED, error);
		}
		if (missingTopic) {
			throw new KafkaError(KAFKA_ERROR_CODES.TOPIC_NOT_PROVISIONED, missingTopic);
		}
	};

	const publish: Kafka<Options>["publish"] = ({
		topic,
		event,
		correlationId,
		headers,
	}) => send({ topic, events: [event], correlationId, headers });

	const publishBatch: Kafka<Options>["publishBatch"] = ({
		topic,
		events,
		correlationId,
		headers,
	}) => send({ topic, events, correlationId, headers });

	const notYet = (name: string) => async () => {
		throw new Error(`kafka.${name} is not implemented yet`);
	};

	return {
		connect,
		disconnect,
		healthCheck,
		provision,
		publish,
		publishBatch,
		consume: notYet("consume"),
		topics,
		options,
		$ERROR_CODES: KAFKA_ERROR_CODES,
	} as unknown as Kafka<Options>;
}

// The old API, until the rewrite replaces it (gatwech-kafka#13)

export { createKafkaClient } from "./core/create-client";
export { connectWithRetry } from "./core/connect-with-retry";
export { DEFAULT_OPTIONS } from "./core/default-options";

export type {
	KafkaClient,
	KafkaClientConfig,
	CreateKafkaClientConfig,
	KafkaExtension,
	GatwechKafkaExtensions,
} from "./core/create-client";

export type { ConnectRetryOptions } from "./core/connect-with-retry";

export { BaseProducer } from "./producers/base";
export {
	createEventPublisher,
	createPublisher,
} from "./producers/event.factory";
export { GenericKafkaProducer } from "./producers/generic-producer";
export { createServicePublisherFactory } from "./producers/service-publisher-factory";

export type { CreatePublisherOptions } from "./producers/event.factory";

export type { EventPublisher } from "./producers/service-publisher-factory";

export type {
	PartitionKeyFn,
	KeyDeriver,
	ReadyToPublishMessage,
} from "./producers/generic-producer";

export { consumeWithRetryAndDLQ } from "./consumers/consume-with-dlq";
export { createDLQPublisher } from "./consumers/dlq.factory";

export type { ConsumeHandlerContext } from "./consumers/consume-with-dlq";

export type {
	DLQContext,
	DLQFailure,
	DLQMessageParts,
	CreateDLQPublisherOptions,
} from "./consumers/dlq.factory";
