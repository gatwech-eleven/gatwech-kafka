import { logger } from "../../utils/logger";
import { Kafka, CompressionTypes } from "kafkajs";
import type {
	Producer,
	ConsumerConfig,
	Transaction,
	Admin,
	ProducerConfig,
	Consumer,
} from "kafkajs";
import type {
	KafkaClientOptions,
	KafkaMetrics,
	TKafkaMessage,
	ResilienceOptions,
	DLQOptions,
} from "../../types";
import type {
	AnyTopic,
	RawTopic,
	SchemaConsumerPayload,
	RawConsumerPayload,
	ConsumerDlqOptions,
} from "../../types/consumer";
import { KafkaError } from "./errors";
import { DEFAULT_OPTIONS } from "./default-options";
import { connectWithRetry } from "./connect-with-retry";
import { createCircuitBreaker, type CircuitBreaker } from "./circuit-breaker";
import {
	createReconnectionMonitor,
	type ReconnectionMonitor,
} from "./reconnection-monitor";
import { initializeKafkaTopics } from "../../config/initialize.topics";
import type {
	TopicConfig,
	ValidatedTopicConfig,
} from "../../config/topics/topic-config-schema";
import {
	consumeWithRetryAndDLQ,
	type ConsumeHandlerContext,
} from "../consumers/consume-with-dlq";
import type { ZodType, z } from "@gatwech/utils/zod";

type AnyTopicConfig = TopicConfig | ValidatedTopicConfig;

const MAX_RETRY_DELAY_MS = 30000;

type OptionalKeys =
	| "ssl"
	| "sasl"
	| "authenticationTimeout"
	| "reauthenticationThreshold";
type ResolvedKafkaClientOptions = Required<
	Omit<KafkaClientOptions, OptionalKeys>
> &
	Pick<KafkaClientOptions, OptionalKeys>;

/**
 * Core Kafka client with connection management, resilience features, and message production.
 * This is the base class that provides all core Kafka functionality.
 *
 * @internal - Use the `createKafkaClient` factory function instead
 */
export class BaseKafkaClient {
	private readonly producer: Producer;
	private readonly kafka: Kafka;
	private readonly options: ResolvedKafkaClientOptions;
	private isConnected = false;
	private connectionRetries = 0;
	private metrics: KafkaMetrics = {
		publishSuccess: 0,
		publishFailures: 0,
		connectionRetries: 0,
		consumerConnections: 0,
	};
	private admin?: Admin;
	private consumers: Map<string, ReturnType<Kafka["consumer"]>> = new Map();
	private readonly defaultMetadata: Record<string, unknown>;
	private readonly defaultHeaders: Record<string, string | Buffer>;
	private readonly _topics?: Record<string, unknown>;

	// Resilience components
	private readonly circuitBreaker?: CircuitBreaker;
	private readonly reconnectionMonitor?: ReconnectionMonitor;
	private readonly resilienceConfig?: ResilienceOptions;
	private lastConnectedAt: Date | null = null;
	private lastDisconnectedAt: Date | null = null;
	private lastConnectionError: string | null = null;

	constructor(
		clientId: string,
		brokers: string[],
		options: KafkaClientOptions = {},
	) {
		if (!clientId) {
			throw new Error("clientId is required");
		}
		this.options = {
			...DEFAULT_OPTIONS,
			...options,
		} as ResolvedKafkaClientOptions;
		this.defaultMetadata = options.defaultMetadata || {};
		this.defaultHeaders = options.defaultHeaders || {};
		this._topics = options.topics;
		this.resilienceConfig = options.resilience;

		this.kafka = new Kafka({
			clientId,
			brokers,
			logLevel: this.options.logLevel,
			requestTimeout: this.options.requestTimeout,
			ssl: this.options.ssl,
			sasl: this.options.sasl,
			connectionTimeout: this.options.connectionTimeout,
			authenticationTimeout: this.options.authenticationTimeout,
			reauthenticationThreshold: this.options.reauthenticationThreshold,
			enforceRequestTimeout: this.options.enforceRequestTimeout,
			retry: {
				maxRetryTime: this.options.retryDelayMs * this.options.maxConnectionRetries,
				retries: this.options.maxConnectionRetries,
			},
		});

		this.producer = this.kafka.producer({
			allowAutoTopicCreation: this.options.allowAutoTopicCreation,
			createPartitioner: this.options.createPartitioner,
			idempotent: true,
			retry: {
				maxRetryTime: this.options.retryDelayMs * this.options.maxConnectionRetries,
				retries: this.options.maxConnectionRetries,
			},
		});

		/**
		 * Initialize resilience components (enabled by default)
		 * Set resilience.enabled = false to disable
		 */
		if (this.resilienceConfig?.enabled !== false) {
			if (this.resilienceConfig?.circuitBreaker?.enabled !== false) {
				this.circuitBreaker = createCircuitBreaker({
					threshold: this.resilienceConfig?.circuitBreaker?.failureThreshold ?? 5,
					resetTimeoutMs:
						this.resilienceConfig?.circuitBreaker?.resetTimeoutMs ?? 30000,
				});
			}

			if (this.resilienceConfig?.reconnection?.enabled !== false) {
				this.reconnectionMonitor = createReconnectionMonitor({
					intervalMs: this.resilienceConfig?.reconnection?.intervalMs ?? 60000,
					logger,
					onReconnect: this.resilienceConfig?.reconnection?.onReconnect,
				});
			}
		}

		this.setupEventListeners();

		/**
		 * Optionally kick off a non-blocking connection on startup
		 */
		if (this.options.autoConnectAsync) {
			this.connectAsync();
		}
	}

	private setupEventListeners(): void {
		this.producer.on("producer.connect", () => {
			this.isConnected = true;
			this.lastConnectedAt = new Date();
			this.circuitBreaker?.recordSuccess();
			logger.info("Kafka producer connected");
		});

		this.producer.on("producer.disconnect", () => {
			this.isConnected = false;
			this.lastDisconnectedAt = new Date();
			logger.info("Kafka producer disconnected");
		});

		this.producer.on("producer.network.request_timeout", (payload) => {
			logger.error("Kafka producer network request timeout", payload);
		});
	}

	private async initializeKafkaConnection(): Promise<void> {
		const timeoutMs =
			this.options.maxConnectAttemptTimeMs ?? this.options.brokerWaitTimeoutMs;
		const pollMs =
			this.options.connectPollIntervalMs ?? this.options.connectPollIntervalMs;
		const timeoutAt = Date.now() + timeoutMs;
		const admin = this.getAdmin();
		try {
			await admin.connect();
			while (Date.now() < timeoutAt) {
				try {
					const info = await admin.describeCluster();
					if (info.controller) return; // ready
				} catch (err) {
					logger.warn("Broker readiness check failed; retrying", {
						error: err,
					});
				}
				await new Promise((r) => setTimeout(r, pollMs));
			}
			throw new KafkaError("Broker readiness timeout", undefined, {
				timeoutMs,
			});
		} finally {
			try {
				await admin.disconnect();
			} catch {}
		}
	}

	public async connect(): Promise<void> {
		if (this.isConnected) return;

		/**
		 * If resilience is explicitly disabled, use original logic
		 */
		if (this.resilienceConfig?.enabled === false) {
			return this.connectLegacy();
		}

		/**
		 * Check circuit breaker
		 */
		if (this.circuitBreaker?.isCircuitOpen()) {
			throw new Error(
				"Circuit breaker is open - too many failed connection attempts. Call resetCircuitBreaker() to retry.",
			);
		}

		/**
		 * Use connectWithRetry for retry logic
		 */
		try {
			await connectWithRetry(
				async () => {
					if (this.options.connectWithRetries || this.options.connectAsync) {
						await this.initializeKafkaConnection();
					}
					await this.producer.connect();
				},
				{
					maxAttempts: this.resilienceConfig?.retry?.maxAttempts ?? 5,
					initialDelayMs: this.resilienceConfig?.retry?.initialDelayMs ?? 1000,
					maxDelayMs: this.resilienceConfig?.retry?.maxDelayMs ?? 30000,
					factor: this.resilienceConfig?.retry?.backoffMultiplier ?? 2,
					onRetry: (attempt) => {
						/**
						 * Track retry attempts in metrics
						 */
						if (this.options.metricsEnabled) {
							this.metrics.connectionRetries++;
						}
					},
				},
			);

			this.isConnected = true;
			this.lastConnectedAt = new Date();
			this.connectionRetries = 0;
			this.lastConnectionError = null;
			this.circuitBreaker?.recordSuccess();

			// Start reconnection monitor
			if (this.reconnectionMonitor) {
				this.reconnectionMonitor.start(
					() => this.isConnected,
					async () => {
						try {
							/**
							 * We call the internal connect logic directly to avoid recursion/circuit breaker checks during background reconnect
							 */
							await this.producer.connect();
							this.isConnected = true;
							this.lastConnectedAt = new Date();
							return true;
						} catch {
							return false;
						}
					},
				);
			}
		} catch (error) {
			this.circuitBreaker?.recordFailure();
			this.lastConnectionError =
				error instanceof Error ? error.message : String(error);
			throw new Error("Failed to connect after all retry attempts");
		}
	}

	private async connectLegacy(): Promise<void> {
		while (this.connectionRetries < this.options.maxConnectionRetries) {
			try {
				if (this.options.connectWithRetries || this.options.connectAsync) {
					await this.initializeKafkaConnection();
				}
				await this.producer.connect();
				this.isConnected = true;
				return;
			} catch (error) {
				this.connectionRetries++;
				this.metrics.connectionRetries++;

				if (this.connectionRetries >= this.options.maxConnectionRetries) {
					logger.error("Max connection retries reached", error);
					throw new KafkaError("Failed to connect to Kafka", error, {
						retries: this.connectionRetries,
					});
				}

				const delay = Math.min(
					this.options.retryDelayMs * 2 ** this.connectionRetries,
					MAX_RETRY_DELAY_MS,
				);
				await new Promise((resolve) => setTimeout(resolve, delay));
			}
		}
	}

	public connectAsync(): void {
		void this.connect().catch((err) => {
			logger.error("Kafka async connect failed", err);
		});
	}

	public async disconnect(): Promise<void> {
		try {
			this.reconnectionMonitor?.stop();

			for (const [groupId, consumer] of this.consumers.entries()) {
				try {
					logger.info("Disconnecting consumer during client shutdown", {
						groupId,
					});
					await consumer.disconnect();
				} catch (err) {
					logger.warn("Error while disconnecting consumer", {
						groupId,
						error: err,
					});
				}
			}

			this.consumers.clear();

			// Only disconnect producer if it was actually connected
			if (this.isConnected) {
				await this.producer.disconnect();
			}

			this.isConnected = false;
			this.lastDisconnectedAt = new Date();

			if (this.admin) {
				await this.admin.disconnect();
			}
		} catch (error) {
			logger.error("Failed to disconnect Kafka client", error);
			throw new KafkaError("Failed to disconnect from Kafka", error);
		}
	}

	public async runTransaction<T>(
		callback: (transaction: Transaction) => Promise<T>,
	): Promise<T> {
		if (!this.isConnected) {
			throw new KafkaError("Producer not connected");
		}

		const transaction = await this.producer.transaction();
		try {
			const result = await callback(transaction);
			await transaction.commit();
			return result;
		} catch (error) {
			await transaction.abort();
			throw new KafkaError("Kafka transaction failed", error);
		}
	}

	public async sendMessages<T>(
		topic: string,
		messages: TKafkaMessage<T>[],
		compression: CompressionTypes = this.options.compression,
	): Promise<void> {
		if (!this.isConnected) {
			throw new KafkaError("Producer not connected");
		}

		if (!topic) {
			throw new Error("topic is required");
		}

		if (!messages || messages.length === 0) {
			throw new Error("messages array cannot be empty");
		}

		messages.forEach((m, i) => {
			if (m.value === undefined || m.value === null) {
				throw new Error(`Message at index ${i} is missing value`);
			}
		});

		// Merge default metadata and headers with message-level ones
		const enrichedMessages = messages.map((m) => ({
			...m,
			metadata: { ...this.defaultMetadata, ...(m.metadata || {}) },
			headers: { ...this.defaultHeaders, ...(m.headers || {}) },
		}));

		try {
			await this.producer.send({
				topic,
				acks: -1,
				messages: enrichedMessages.map((m) => ({
					key: m.key,
					value: JSON.stringify(m.value),
					headers: m.headers,
					timestamp: m.timestamp,
					partition: m.partition,
				})),
				compression,
			});

			if (this.options.metricsEnabled) {
				this.metrics.publishSuccess += messages.length;
			}
			logger.debug(
				`Successfully sent ${messages.length} messages to topic: ${topic}`,
			);
		} catch (error) {
			if (this.options.metricsEnabled) {
				this.metrics.publishFailures += messages.length;
			}
			logger.error(`Failed to send messages to topic "${topic}"`, error);
			throw new KafkaError("Messages send failed", error, { topic });
		}
	}

	public async sendMessage<T>(
		topic: string,
		message: TKafkaMessage<T>,
		compression: CompressionTypes = this.options.compression,
	): Promise<void> {
		return this.sendMessages(topic, [message], compression);
	}

	public getAdmin(): Admin {
		if (!this.admin) {
			this.admin = this.kafka.admin();
		}
		return this.admin;
	}

	public createProducer(overrides?: Partial<Parameters<Kafka["producer"]>[0]>) {
		const opts = {
			allowAutoTopicCreation: this.options.allowAutoTopicCreation,
			createPartitioner: this.options.createPartitioner,
			idempotent: true,
			retry: {
				maxRetryTime: this.options.retryDelayMs * this.options.maxConnectionRetries,
				retries: this.options.maxConnectionRetries,
			},
			...(overrides || {}),
		} as ProducerConfig;

		const p = this.kafka.producer(opts);

		return p;
	}

	public createConsumer(config: ConsumerConfig) {
		return this.kafka.consumer(config);
	}

	/**
	 * Consume messages from a topic with automatic retry and DLQ handling.
	 * Validates messages against the topic schema if provided.
	 *
	 * @param groupId - Consumer group ID
	 * @param topic - Topic configuration with optional schema
	 * @param handler - Message handler function (payload type inferred from schema)
	 * @param opts - Optional DLQ configuration
	 * @returns Consumer instance
	 *
	 * @example
	 * // With schema (typed payload)
	 * await client.consume(
	 *   'my-group',
	 *   userTopic, // has schema: z.object({id: z.string()})
	 *   async (payload, ctx) => {
	 *     console.log(payload.id); // TypeScript knows this is a string
	 *   }
	 * );
	 *
	 * @example
	 * // Without schema (raw payload)
	 * await client.consume(
	 *   'my-group',
	 *   rawTopic,
	 *   async (payload, ctx) => {
	 *     console.log(payload.message.value); // EachMessagePayload
	 *   }
	 * );
	 */
	public consume<TSchema extends ZodType>(
		groupId: string,
		topic: TopicConfig & { schema: TSchema },
		handler: (
			payload: SchemaConsumerPayload<TSchema>,
			ctx: ConsumeHandlerContext,
		) => Promise<void>,
		opts?: ConsumerDlqOptions,
	): Promise<Consumer>;

	public consume<TRawTopic extends RawTopic>(
		groupId: string,
		topic: TRawTopic,
		handler: (
			payload: RawConsumerPayload,
			ctx: ConsumeHandlerContext,
		) => Promise<void>,
		opts?: ConsumerDlqOptions,
	): Promise<Consumer>;

	public async consume(
		groupId: string,
		topic: AnyTopic,
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		handler: any,
		opts?: ConsumerDlqOptions,
	): Promise<Consumer> {
		// Cast topic to any to bypass overload resolution since implementation handles AnyTopic
		return consumeWithRetryAndDLQ(this, groupId, topic as any, handler, opts);
	}

	public getProducer(): Producer {
		return this.producer;
	}

	public getMetrics(): KafkaMetrics {
		return { ...this.metrics };
	}

	public isReady(): boolean {
		return this.isConnected;
	}

	public getDefaultMetadata(): Readonly<Record<string, unknown>> {
		return { ...this.defaultMetadata };
	}

	public getDefaultHeaders(): Readonly<Record<string, string | Buffer>> {
		return { ...this.defaultHeaders };
	}

	public get topics(): Readonly<Record<string, any>> | undefined {
		return this._topics;
	}

	public async healthCheck(): Promise<boolean> {
		try {
			if (!this.isConnected) {
				await this.connect();
			}
			const admin = this.getAdmin();
			await admin.connect();
			const clusterInfo = await admin.describeCluster();
			await admin.disconnect();
			return !!clusterInfo.controller;
		} catch (error) {
			logger.error("Kafka health check failed", error);
			return false;
		}
	}

	public getConnectionStatus() {
		return {
			connected: this.isConnected,
			connectionAttempts: this.connectionRetries,
			lastError: this.lastConnectionError,
			lastConnectedAt: this.lastConnectedAt,
			lastDisconnectedAt: this.lastDisconnectedAt,
			circuitBreakerOpen: this.circuitBreaker?.isCircuitOpen() ?? false,
		};
	}

	public getHealthStatus() {
		const status = this.getConnectionStatus();

		const uptime = status.lastConnectedAt
			? Date.now() - status.lastConnectedAt.getTime()
			: null;

		let healthStatus: "healthy" | "degraded" | "unhealthy";

		if (status.connected) {
			healthStatus = "healthy";
		} else if (status.circuitBreakerOpen) {
			healthStatus = "unhealthy";
		} else {
			healthStatus = "degraded";
		}

		return {
			status: healthStatus,
			connected: status.connected,
			details: {
				connectionAttempts: status.connectionAttempts,
				lastError: status.lastError,
				uptime,
			},
		};
	}

	public resetCircuitBreaker(): void {
		this.circuitBreaker?.reset();
		logger.info("Circuit breaker manually reset");
	}

	public async reconnect(): Promise<boolean> {
		try {
			await this.connect();
			return true;
		} catch {
			return false;
		}
	}

	public async provisionTopics(): Promise<void> {
		if (!this._topics) {
			logger.warn("No topics configured for provisioning");
			return;
		}

		const options = this.options.topicProvisioning || {};
		const admin = this.getAdmin();

		await initializeKafkaTopics(admin, {
			topics: this._topics as Record<string, AnyTopicConfig>,
			partitions: options.partitions,
			replicationFactor: options.replicationFactor,
			waitForLeadersMs: options.waitForLeadersMs,
		});
	}
}
