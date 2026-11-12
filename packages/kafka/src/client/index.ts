import { createLogger, type InternalLogger } from "@gatwech/core/env";
import kafkajs, {
	CompressionTypes,
	Kafka as KafkaJS,
	type LogEntry,
	logLevel,
	Partitioners,
} from "kafkajs";
import { createMessageHandler, getBackoffMs } from "../consumer";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import { defaultDlqTopicName, resolveTopics } from "../topics";
import { provisionTopics } from "../topics/provision";
import { buildMessages } from "./build-messages";
import { createCircuitBreaker } from "./circuit-breaker";
import { createReconnectionMonitor } from "./reconnection";
import type { InferTopics, Kafka, KafkaOptions } from "./types";

export type * from "./types";

const sleep = (ms: number) =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

const messageOf = (error: unknown) =>
	error instanceof Error ? error.message : String(error);

// Node's ESM finds only some of kafkajs's exports by name, so these two are
// read from the default export, which is the whole module
const { KafkaJSNumberOfRetriesExceeded, KafkaJSProtocolError } = kafkajs;

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

	// Each topic and the DLQ its messages go to, as the broker knows them. A DLQ
	// that is also declared as a topic, to be consumed, keeps its declaration
	const provisioned = Object.values(topics);
	for (const topic of Object.values(options.topics ?? {})) {
		const name = defaultDlqTopicName(topic, options.namespace);
		if (!provisioned.some((declared) => declared.name === name)) {
			provisioned.push({
				...topic,
				name,
				partitions: 1,
				cleanupPolicy: "delete",
			});
		}
	}

	let isConnected = false;
	let isClosed = false;
	let connecting: Promise<void> | undefined;
	const consumers = new Set<{ disconnect: () => Promise<void> }>();

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
		if (!isEnabled || provisioned.length === 0) {
			return;
		}
		if (!isConnected) {
			throw new KafkaError(KAFKA_ERROR_CODES.NOT_CONNECTED);
		}
		await provisionTopics({
			admin,
			logger,
			topics: provisioned,
			partitions: options.provisioning?.partitions ?? 3,
			replicationFactor: options.provisioning?.replicationFactor ?? 1,
			waitForLeadersMs: options.provisioning?.waitForLeadersMs ?? 15_000,
		});
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
		// The consumers go first: the DLQ is published with the producer
		const leavings = await Promise.allSettled(
			[...consumers].map((consuming) => consuming.disconnect()),
		);
		const disconnections = [...leavings, ...(await closeClients())];
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

	const consume: Kafka<Options>["consume"] = async (consumeOptions) => {
		const groupId =
			consumeOptions.groupId ?? `${options.clientId}.${consumeOptions.topic}`;
		if (!isEnabled) {
			logger.warn("Kafka is disabled; not consuming", {
				topic: consumeOptions.topic,
			});
			return {
				groupId,
				paused: () => [],
				resume: () => {},
				disconnect: async () => {},
			};
		}
		const topic = topics[consumeOptions.topic];
		if (!topic) {
			throw new KafkaError(
				KAFKA_ERROR_CODES.TOPIC_NOT_DECLARED,
				consumeOptions.topic,
			);
		}
		if (!isConnected) {
			throw new KafkaError(KAFKA_ERROR_CODES.NOT_CONNECTED);
		}

		const retry = {
			attempts: Math.max(1, consumeOptions.retry?.attempts ?? 3),
			backoffMs: Math.max(1, consumeOptions.retry?.backoffMs ?? 500),
		};
		const session = {
			timeoutMs: consumeOptions.session?.timeoutMs ?? 30_000,
			heartbeatIntervalMs: consumeOptions.session?.heartbeatIntervalMs ?? 3_000,
			rebalanceTimeoutMs: consumeOptions.session?.rebalanceTimeoutMs ?? 60_000,
		};
		if (session.heartbeatIntervalMs >= session.timeoutMs) {
			throw new KafkaError(KAFKA_ERROR_CODES.HEARTBEAT_NOT_BELOW_SESSION_TIMEOUT);
		}
		// A handler that retries sends no heartbeat, so the broker would remove
		// the consumer from its group in the middle of the back-off
		let totalBackoffMs = 0;
		for (let failures = 1; failures < retry.attempts; failures++) {
			totalBackoffMs += getBackoffMs(retry.backoffMs, failures);
		}
		if (totalBackoffMs >= session.timeoutMs) {
			throw new KafkaError(KAFKA_ERROR_CODES.RETRY_EXCEEDS_SESSION_TIMEOUT);
		}

		const consumer = client.consumer({
			groupId,
			sessionTimeout: session.timeoutMs,
			heartbeatInterval: session.heartbeatIntervalMs,
			rebalanceTimeout: session.rebalanceTimeoutMs,
			allowAutoTopicCreation: false,
		});
		consumer.on(consumer.events.CRASH, ({ payload }) => {
			logger.error("Kafka consumer crashed", {
				groupId,
				error: messageOf(payload.error),
				restarting: payload.restart,
			});
		});

		try {
			await consumer.connect();
			await consumer.subscribe({
				topic: topic.name,
				fromBeginning: consumeOptions.fromBeginning ?? false,
			});
			await consumer.run({
				eachMessage: createMessageHandler({
					topic,
					groupId,
					logger,
					getProducer: () => producer,
					// The handler is typed by the schema of the topic it reads
					handler: consumeOptions.handler as Parameters<
						typeof createMessageHandler
					>[0]["handler"],
					retry,
					dlq: {
						topic: consumeOptions.dlq?.topic ?? `${topic.name}.dlq`,
						attempts: Math.max(1, consumeOptions.dlq?.attempts ?? 3),
					},
				}),
			});
		} catch (error) {
			await consumer.disconnect().catch(() => undefined);
			throw new KafkaError(KAFKA_ERROR_CODES.CONSUMER_START_FAILED, error);
		}

		const consuming = {
			groupId,
			paused: () => consumer.paused(),
			resume: () => consumer.resume(consumer.paused()),
			disconnect: async () => {
				consumers.delete(consuming);
				await consumer.disconnect();
			},
		};
		consumers.add(consuming);
		logger.info("Consuming", { topic: topic.name, groupId });
		return consuming;
	};

	return {
		connect,
		disconnect,
		healthCheck,
		provision,
		publish,
		publishBatch,
		consume,
		topics: topics as InferTopics<Options>,
		options,
		$ERROR_CODES: KAFKA_ERROR_CODES,
	};
}
