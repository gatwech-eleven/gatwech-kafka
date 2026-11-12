import { z } from "@gatwech/utils/zod";
import {
	afterAll,
	afterEach,
	beforeAll,
	describe,
	expect,
	expectTypeOf,
	it,
	vi,
} from "vitest";
import type { KafkaConsumer } from "../consumer/types";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import { getTestInstance } from "../test-utils/test-instance";
import { defineTopics } from "../topics";
import { createKafka } from ".";
import { createCircuitBreaker } from "./circuit-breaker";
import { createReconnectionMonitor } from "./reconnection";
import type { KafkaOptions } from "./types";

/**
 * The contract is types, so these are checked by `tsc` and never run:
 * `createKafka` is a declaration until its module lands
 */
describe("client - public types", () => {
	const userRegistered = {
		name: "user.registered",
		schema: z.object({ userId: z.string(), email: z.string() }),
		key: (event: { userId: string }) => event.userId,
	};

	const clientWith = () =>
		createKafka({
			clientId: "payvo-auth-service",
			brokers: ["localhost:9092"],
			topics: defineTopics({ userRegistered }),
		});

	it("types an event from the schema of the topic it names", () => {
		const check = () => {
			const kafka = clientWith();

			expectTypeOf(kafka.publish).returns.resolves.toBeVoid();
			void kafka.publish({
				topic: "userRegistered",
				event: { userId: "u1", email: "a@b.c" },
			});
			void kafka.publish({
				topic: "userRegistered",
				// @ts-expect-error email is a string
				event: { userId: "u1", email: 1 },
			});
			void kafka.publish({
				topic: "userRegistered",
				// @ts-expect-error userId is missing
				event: { email: "a@b.c" },
			});
			// @ts-expect-error the event goes under `event`
			void kafka.publish({ topic: "userRegistered", userId: "u1" });
		};
		void check;
	});

	it("types every event of a batch from the same schema", () => {
		const check = () => {
			const kafka = clientWith();

			void kafka.publishBatch({
				topic: "userRegistered",
				events: [
					{ userId: "u1", email: "a@b.c" },
					{ userId: "u2", email: "d@e.f" },
				],
			});
			void kafka.publishBatch({
				topic: "userRegistered",
				// @ts-expect-error the second event has no email
				events: [{ userId: "u1", email: "a@b.c" }, { userId: "u2" }],
			});
		};
		void check;
	});

	it("only names topics the client was given", () => {
		const check = () => {
			const kafka = clientWith();

			// @ts-expect-error there is no such topic
			void kafka.publish({ topic: "userDeleted", event: {} });
			void kafka.consume({
				// @ts-expect-error there is no such topic
				topic: "userDeleted",
				handler: async () => {},
			});

			const bare = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
			});
			// @ts-expect-error a client with no topics has none to name
			void bare.publish({ topic: "userRegistered", event: {} });
		};
		void check;
	});

	it("types the event a handler receives from the same schema", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-wallet-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string(), age: z.coerce.number() }),
						key: (event) => event.userId,
					},
				}),
			});

			const consumer = kafka.consume({
				topic: "userRegistered",
				handler: async ({ event, partition, topic, correlationId, source }) => {
					// The handler gets what the schema outputs, so `age` is parsed
					expectTypeOf(event).toEqualTypeOf<{ userId: string; age: number }>();
					expectTypeOf(partition).toBeNumber();
					expectTypeOf(topic).toBeString();
					expectTypeOf(correlationId).toEqualTypeOf<string | undefined>();
					expectTypeOf(source).toEqualTypeOf<string | undefined>();
					// @ts-expect-error the schema has no such field
					void event.nope;
				},
			});
			expectTypeOf(consumer).resolves.toEqualTypeOf<KafkaConsumer>();
		};
		void check;
	});

	it("reads a topic with nothing but the topic and a handler", () => {
		const check = () => {
			const kafka = clientWith();

			void kafka.consume({
				topic: "userRegistered",
				handler: async ({ event }) => {
					expectTypeOf(event.userId).toBeString();
				},
			});
			void kafka.consume({
				topic: "userRegistered",
				groupId: "payvo-auth-service.legacy-group",
				retry: { attempts: 5 },
				dlq: { publishAttempts: 2 },
				session: { timeoutMs: 45_000 },
				handler: async () => {},
			});
		};
		void check;
	});

	it("types a topic's key from its event", () => {
		const check = () => {
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					// @ts-expect-error the event has no `id`
					key: (event) => event.id,
				},
			});
		};
		void check;
	});

	it("lets a topic keep its declared name inside a namespace", () => {
		const check = () => {
			// The auth service's topics, as they are on the broker: two carry the
			// namespace and the rest are bare
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				namespace: "prod",
				topics: defineTopics({
					userRegistered: { ...userRegistered, namespaced: false },
					kycStatusUpdated: {
						name: "user.kyc.status.updated",
						schema: z.object({ userId: z.string(), status: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});

			expectTypeOf(kafka.topics.userRegistered.namespaced).toBeBoolean();
			void kafka.publish({
				topic: "kycStatusUpdated",
				event: { userId: "u1", status: "approved" },
			});
		};
		void check;
	});

	it("keeps the topics' types on the instance", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({ userRegistered }),
				namespace: "prod",
			});

			expectTypeOf(kafka.topics.userRegistered.name).toBeString();
			expectTypeOf(kafka.topics.userRegistered.retentionMs).toBeNumber();
		};
		void check;
	});

	it("needs a client id and brokers, and nothing else", () => {
		const check = () => {
			createKafka({ clientId: "payvo-auth-service", brokers: ["a:9092"] });
			createKafka({
				clientId: "payvo-auth-service",
				brokers: ["a:9092"],
				enabled: false,
			});
			// @ts-expect-error brokers are required
			createKafka({ clientId: "payvo-auth-service" });
			// @ts-expect-error clientId is required
			createKafka({ brokers: ["a:9092"] });
		};
		void check;
	});

	it("exposes its error codes on the instance", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["a:9092"],
			});
			expectTypeOf(kafka.$ERROR_CODES).toEqualTypeOf<typeof KAFKA_ERROR_CODES>();
			expectTypeOf(kafka.$ERROR_CODES.BROKER_UNREACHABLE).toBeString();
		};
		void check;
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/29
 */
describe("client - circuit breaker", () => {
	const setup = (overrides: { enabled?: boolean } = {}) => {
		let time = 0;
		const breaker = createCircuitBreaker({
			enabled: overrides.enabled ?? true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
			now: () => time,
		});
		return {
			breaker,
			advance: (ms: number) => {
				time += ms;
			},
		};
	};
	const fails = () => Promise.reject(new Error("broker down"));
	const works = () => Promise.resolve("sent");

	it("should stay closed below the failure threshold", async () => {
		const { breaker } = setup();

		await expect(breaker.run(fails)).rejects.toThrow("broker down");

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(works)).resolves.toBe("sent");
	});

	it("should count only the calls that fail in a row", async () => {
		const { breaker } = setup();

		await expect(breaker.run(fails)).rejects.toThrow();
		await breaker.run(works);
		await expect(breaker.run(fails)).rejects.toThrow();

		expect(breaker.state()).toBe("closed");
	});

	it("should open at the threshold and refuse calls without making them", async () => {
		const { breaker } = setup();
		await expect(breaker.run(fails)).rejects.toThrow();
		await expect(breaker.run(fails)).rejects.toThrow();
		const call = vi.fn(works);

		const refused = await breaker.run(call).catch((error) => error);

		expect(breaker.state()).toBe("open");
		expect(refused).toBeInstanceOf(KafkaError);
		expect(refused.message).toBe(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
		expect(call).not.toHaveBeenCalled();
	});

	it("should let exactly one call through once the reset timeout has passed", async () => {
		const { breaker, advance } = setup();
		await expect(breaker.run(fails)).rejects.toThrow();
		await expect(breaker.run(fails)).rejects.toThrow();
		advance(1_000);
		expect(breaker.state()).toBe("half-open");

		let finishTrial!: () => void;
		const trial = breaker.run(
			() =>
				new Promise<string>((resolve) => (finishTrial = () => resolve("sent"))),
		);
		const others = vi.fn(works);
		const refused = await breaker.run(others).catch((error) => error);

		expect(refused.message).toBe(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
		expect(others).not.toHaveBeenCalled();
		finishTrial();
		await expect(trial).resolves.toBe("sent");
	});

	it("should close when the call it let through works", async () => {
		const { breaker, advance } = setup();
		await expect(breaker.run(fails)).rejects.toThrow();
		await expect(breaker.run(fails)).rejects.toThrow();
		advance(1_000);

		await expect(breaker.run(works)).resolves.toBe("sent");

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(works)).resolves.toBe("sent");
	});

	it("should open again when the call it let through fails", async () => {
		const { breaker, advance } = setup();
		await expect(breaker.run(fails)).rejects.toThrow();
		await expect(breaker.run(fails)).rejects.toThrow();
		advance(1_000);

		await expect(breaker.run(fails)).rejects.toThrow("broker down");

		expect(breaker.state()).toBe("open");
		const call = vi.fn(works);
		await expect(breaker.run(call)).rejects.toThrow(
			KAFKA_ERROR_CODES.CIRCUIT_OPEN,
		);
		expect(call).not.toHaveBeenCalled();
		advance(999);
		expect(breaker.state()).toBe("open");
		advance(1);
		expect(breaker.state()).toBe("half-open");
	});

	it("should only call the function when it is disabled", async () => {
		const { breaker } = setup({ enabled: false });

		for (let call = 0; call < 5; call++) {
			await expect(breaker.run(fails)).rejects.toThrow("broker down");
		}

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(works)).resolves.toBe("sent");
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/25
 */
describe("client - reconnection monitor", () => {
	afterEach(() => {
		vi.useFakeTimers();
	});

	const setup = (check: () => Promise<boolean>, reconnect = async () => {}) => {
		const reconnectFn = vi.fn(reconnect);
		const onError = vi.fn();
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check,
			reconnect: reconnectFn,
			onError,
		});
		return { monitor, reconnect: reconnectFn, onError };
	};

	it("should reconnect when the connection is down", async () => {
		const { monitor, reconnect } = setup(async () => false);

		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(reconnect).toHaveBeenCalledTimes(1);
		monitor.stop();
	});

	it("should leave a working connection alone", async () => {
		const { monitor, reconnect } = setup(async () => true);

		monitor.start();
		await vi.advanceTimersByTimeAsync(5_000);

		expect(reconnect).not.toHaveBeenCalled();
		monitor.stop();
	});

	it("should not start a check or a reconnection while one is running", async () => {
		let finish!: () => void;
		const { monitor, reconnect } = setup(
			async () => false,
			() => new Promise<void>((resolve) => (finish = resolve)),
		);

		monitor.start();
		await vi.advanceTimersByTimeAsync(5_000);
		expect(reconnect).toHaveBeenCalledTimes(1);

		finish();
		await vi.advanceTimersByTimeAsync(1_000);
		expect(reconnect).toHaveBeenCalledTimes(2);
		monitor.stop();
	});

	it("should report what a reconnection throws and try again on the next tick", async () => {
		const failure = new Error("still down");
		const { monitor, reconnect, onError } = setup(
			async () => false,
			async () => {
				throw failure;
			},
		);

		monitor.start();
		await vi.advanceTimersByTimeAsync(2_000);

		expect(onError).toHaveBeenCalledWith(failure);
		expect(reconnect).toHaveBeenCalledTimes(2);
		monitor.stop();
	});

	it("should report what a check throws", async () => {
		const failure = new Error("cannot tell");
		const { monitor, reconnect, onError } = setup(async () => {
			throw failure;
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(onError).toHaveBeenCalledWith(failure);
		expect(reconnect).not.toHaveBeenCalled();
		monitor.stop();
	});

	it("should stop checking once stopped", async () => {
		const { monitor, reconnect } = setup(async () => false);
		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		monitor.stop();
		await vi.advanceTimersByTimeAsync(5_000);

		expect(reconnect).toHaveBeenCalledTimes(1);
		expect(monitor.isRunning()).toBe(false);
	});

	it("should run one timer however often it is started", async () => {
		const { monitor, reconnect } = setup(async () => false);

		monitor.start();
		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(reconnect).toHaveBeenCalledTimes(1);
		monitor.stop();
	});

	it("should not keep the process alive", () => {
		const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
		const { monitor } = setup(async () => true);

		monitor.start();

		const timer = setIntervalSpy.mock.results[0]?.value as NodeJS.Timeout;
		expect(timer.hasRef()).toBe(false);
		monitor.stop();
		setIntervalSpy.mockRestore();
	});
});

const slow = 30_000;

const createCapturingLogger = () => {
	const logs: { level: string; message: string }[] = [];
	return {
		logs,
		logger: {
			level: "debug" as const,
			log: (level: string, message: string) => {
				logs.push({ level, message });
			},
		},
		count: (message: string) =>
			logs.filter((entry) => entry.message === message).length,
	};
};

describe("client - connection", () => {
	let instance: Awaited<ReturnType<typeof getTestInstance>>;
	let topicNumber = 0;

	beforeAll(async () => {
		vi.useRealTimers();
		instance = await getTestInstance();
	});

	afterAll(async () => {
		await instance.close();
		vi.useFakeTimers();
	});

	const nextTopicName = () => `user.registered.${++topicNumber}`;
	const schema = z.object({ userId: z.string() });
	const key = (event: { userId: string }) => event.userId;

	const setup = (overrides: Partial<KafkaOptions> = {}) => {
		const capture = createCapturingLogger();
		const kafka = instance.track(
			createKafka({
				clientId: `payvo-test-${instance.namespace}`,
				brokers: instance.brokers,
				namespace: instance.namespace,
				connect: { maxAttempts: 2, retryDelayMs: 50 },
				logger: capture.logger,
				...overrides,
			}),
		);
		return { kafka, ...capture };
	};

	describe("connect", () => {
		it("should connect and be healthy", async () => {
			const { kafka } = setup();

			await kafka.connect();

			expect(await kafka.healthCheck()).toEqual({
				healthy: true,
				connected: true,
				circuit: "closed",
			});
		});

		it("should make one connection for calls made at once, and for a later one", async () => {
			const { kafka, count } = setup();

			await Promise.all([kafka.connect(), kafka.connect()]);
			await kafka.connect();

			expect(count("Connected to Kafka")).toBe(1);
		});

		it("should open no connection until connect is called", async () => {
			const { count } = setup({ brokers: ["localhost:1"] });

			await new Promise((resolve) => setTimeout(resolve, 200));

			expect(count("Could not connect to Kafka")).toBe(0);
		});

		it(
			"should throw BROKER_UNREACHABLE after exactly connect.maxAttempts tries",
			async () => {
				const { kafka, count } = setup({
					brokers: ["localhost:1"],
					connect: { maxAttempts: 3, retryDelayMs: 10 },
				});

				const startedAt = Date.now();
				const error = await kafka.connect().catch((reason) => reason);

				expect(error).toBeInstanceOf(KafkaError);
				expect(error.message).toBe(KAFKA_ERROR_CODES.BROKER_UNREACHABLE);
				expect(error.cause).toBeDefined();
				expect(count("Could not connect to Kafka")).toBe(3);
				// kafkajs would retry each of the three by itself, for about 8 seconds
				expect(Date.now() - startedAt).toBeLessThan(3_000);
			},
			slow,
		);

		it(
			"should give up on a host that can't be found like on any other failure",
			async () => {
				const { kafka, count } = setup({
					brokers: ["nonexistent.invalid:9092"],
					connect: { maxAttempts: 2, retryDelayMs: 10 },
				});

				const error = await kafka.connect().catch((reason) => reason);

				expect(error.message).toBe(KAFKA_ERROR_CODES.BROKER_UNREACHABLE);
				expect(count("Could not connect to Kafka")).toBe(2);
			},
			slow,
		);

		it(
			"should stop trying when disconnect is called while it connects",
			async () => {
				const { kafka, count } = setup({
					brokers: ["localhost:1"],
					connect: { maxAttempts: 50, retryDelayMs: 100 },
					reconnection: { enabled: false },
				});

				const connecting = kafka.connect();
				await new Promise((resolve) => setTimeout(resolve, 250));
				await kafka.disconnect();

				await expect(connecting).resolves.toBeUndefined();
				expect(count("Could not connect to Kafka")).toBeLessThan(10);
			},
			slow,
		);
	});

	describe("reconnection", () => {
		it("should keep trying in the background after the first connect fails", async () => {
			const { kafka, count } = setup({
				brokers: ["localhost:1"],
				connect: { maxAttempts: 1, retryDelayMs: 10 },
				reconnection: { intervalMs: 100 },
			});
			await kafka.connect().catch(() => undefined);
			expect(count("Could not connect to Kafka")).toBe(1);

			await vi.waitFor(
				() => expect(count("Could not connect to Kafka")).toBeGreaterThanOrEqual(3),
				{ timeout: 5_000, interval: 50 },
			);
			await kafka.disconnect();
		});

		it("should stop trying once disconnected", async () => {
			const { kafka, count } = setup({
				brokers: ["localhost:1"],
				connect: { maxAttempts: 1, retryDelayMs: 10 },
				reconnection: { intervalMs: 100 },
			});
			await kafka.connect().catch(() => undefined);
			await kafka.disconnect();
			const attempts = count("Could not connect to Kafka");

			await new Promise((resolve) => setTimeout(resolve, 600));

			expect(count("Could not connect to Kafka")).toBe(attempts);
		});

		it("should release its timer once disconnected", async () => {
			const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
			const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");
			const { kafka } = setup({
				brokers: ["localhost:1"],
				connect: { maxAttempts: 1, retryDelayMs: 10 },
				reconnection: { intervalMs: 123 },
			});
			await kafka.connect().catch(() => undefined);
			const monitorCall = setIntervalSpy.mock.calls.findIndex(
				([, intervalMs]) => intervalMs === 123,
			);
			const timer = setIntervalSpy.mock.results[monitorCall]?.value;

			await kafka.disconnect();

			expect(monitorCall).toBeGreaterThanOrEqual(0);
			expect(clearIntervalSpy).toHaveBeenCalledWith(timer);
			setIntervalSpy.mockRestore();
			clearIntervalSpy.mockRestore();
		});

		it("should not reconnect when reconnection is off", async () => {
			const { kafka, count } = setup({
				brokers: ["localhost:1"],
				connect: { maxAttempts: 1, retryDelayMs: 10 },
				reconnection: { enabled: false, intervalMs: 100 },
			});
			await kafka.connect().catch(() => undefined);

			await new Promise((resolve) => setTimeout(resolve, 500));

			expect(count("Could not connect to Kafka")).toBe(1);
		});
	});

	describe("provision", () => {
		it(
			"should create each topic with its settings, and a DLQ for each",
			async () => {
				const name = nextTopicName();
				const { kafka } = setup({
					topics: defineTopics({
						userRegistered: {
							name,
							schema,
							key,
							partitions: 2,
							retentionMs: 60_000,
							cleanupPolicy: "delete",
						},
					}),
				});

				await kafka.connect();

				const topic = `${instance.namespace}.${name}`;
				expect(await instance.describeTopic(topic)).toEqual({
					partitions: 2,
					retentionMs: 60_000,
					cleanupPolicy: "delete",
				});
				expect(await instance.describeTopic(`${topic}.dlq`)).toEqual({
					partitions: 1,
					retentionMs: 60_000,
					cleanupPolicy: "delete",
				});
			},
			slow,
		);

		it(
			"should keep the declared name of a topic that opts out of the namespace",
			async () => {
				const name = instance.uniqueName("user.created");
				const { kafka } = setup({
					topics: defineTopics({
						userCreated: { name, namespaced: false, schema, key },
					}),
				});

				await kafka.connect();

				expect(await instance.describeTopic(name)).toBeDefined();
				expect(await instance.describeTopic(`${name}.dlq`)).toBeDefined();
				expect(
					await instance.describeTopic(`${instance.namespace}.${name}`),
				).toBeUndefined();
			},
			slow,
		);

		it(
			"should use provisioning for what a topic leaves out",
			async () => {
				const name = nextTopicName();
				const { kafka } = setup({
					provisioning: { partitions: 2 },
					topics: defineTopics({ userRegistered: { name, schema, key } }),
				});

				await kafka.connect();

				expect(
					(await instance.describeTopic(`${instance.namespace}.${name}`))
						?.partitions,
				).toBe(2);
			},
			slow,
		);

		it(
			"should leave topics that exist alone, and be callable again",
			async () => {
				const name = nextTopicName();
				const topics = defineTopics({
					userRegistered: { name, schema, key, retentionMs: 60_000 },
				});
				const first = setup({ topics });
				await first.kafka.connect();
				await first.kafka.disconnect();

				const second = setup({ topics });
				await second.kafka.connect();
				await second.kafka.provision();

				expect(
					await instance.describeTopic(`${instance.namespace}.${name}`),
				).toMatchObject({ retentionMs: 60_000 });
			},
			slow,
		);

		it("should refuse to provision before it is connected", async () => {
			const { kafka } = setup({
				topics: defineTopics({
					userRegistered: { name: nextTopicName(), schema, key },
				}),
			});

			await expect(kafka.provision()).rejects.toThrow(
				KAFKA_ERROR_CODES.NOT_CONNECTED,
			);
		});

		it("should create nothing for a client without topics", async () => {
			const namespace = `${instance.namespace}.none`;
			const { kafka } = setup({ namespace });

			await kafka.connect();

			expect(await instance.listTopics(`${namespace}.`)).toEqual([]);
		});
	});

	describe("healthCheck", () => {
		it("should report a client that isn't connected without connecting it", async () => {
			const { kafka, count } = setup();

			const first = await kafka.healthCheck();
			const second = await kafka.healthCheck();

			expect(first).toEqual({
				healthy: false,
				connected: false,
				circuit: "closed",
			});
			expect(second).toEqual(first);
			expect(count("Connected to Kafka")).toBe(0);
		});

		it(
			"should leave the client usable for what comes after it",
			async () => {
				const name = nextTopicName();
				const { kafka } = setup({
					topics: defineTopics({ userRegistered: { name, schema, key } }),
				});
				await kafka.connect();

				await kafka.healthCheck();
				await kafka.healthCheck();
				await kafka.provision();

				expect(await kafka.healthCheck()).toMatchObject({ healthy: true });
				expect(
					await instance.describeTopic(`${instance.namespace}.${name}`),
				).toBeDefined();
			},
			slow,
		);

		it("should report a client that has disconnected", async () => {
			const { kafka } = setup();
			await kafka.connect();

			await kafka.disconnect();

			expect(await kafka.healthCheck()).toMatchObject({
				healthy: false,
				connected: false,
			});
		});
	});

	describe("disconnect", () => {
		it("should not throw when it is called twice, or before connect", async () => {
			const { kafka } = setup();

			await expect(kafka.disconnect()).resolves.toBeUndefined();
			await kafka.connect();
			await expect(kafka.disconnect()).resolves.toBeUndefined();
			await expect(kafka.disconnect()).resolves.toBeUndefined();
		});

		it("should connect again after it disconnected", async () => {
			const { kafka } = setup();
			await kafka.connect();
			await kafka.disconnect();

			await kafka.connect();

			expect(await kafka.healthCheck()).toMatchObject({ healthy: true });
		});
	});

	describe("enabled", () => {
		it(
			"should neither connect nor create topics when it is off, whatever the brokers",
			async () => {
				const namespace = `${instance.namespace}.disabled`;
				const { kafka, count } = setup({
					enabled: false,
					namespace,
					brokers: ["localhost:1"],
					topics: defineTopics({
						userRegistered: { name: nextTopicName(), schema, key },
					}),
				});

				await expect(kafka.connect()).resolves.toBeUndefined();
				await expect(kafka.provision()).resolves.toBeUndefined();
				await expect(kafka.disconnect()).resolves.toBeUndefined();

				expect(await kafka.healthCheck()).toEqual({
					healthy: false,
					connected: false,
					circuit: "closed",
				});
				expect(count("Could not connect to Kafka")).toBe(0);
				expect(await instance.listTopics(`${namespace}.`)).toEqual([]);
			},
			slow,
		);
	});

	describe("options", () => {
		it("should refuse a client with no brokers", () => {
			expect(() => createKafka({ clientId: "payvo-test", brokers: [] })).toThrow(
				KAFKA_ERROR_CODES.BROKERS_REQUIRED,
			);
		});

		it("should read no environment variable of its own", () => {
			const read: string[] = [];
			const original = process.env;
			process.env = new Proxy(original, {
				get: (target, name) => {
					read.push(String(name));
					return target[name as string];
				},
			});
			try {
				createKafka({
					clientId: "payvo-test",
					brokers: ["localhost:1"],
					// The logger asks the terminal about colours unless told not to
					logger: { disabled: true, disableColors: true },
				});
			} finally {
				process.env = original;
			}

			// kafkajs looks at a few KAFKAJS_ variables of its own
			expect(read.filter((name) => !name.startsWith("KAFKAJS_"))).toEqual([]);
		});
	});
});
