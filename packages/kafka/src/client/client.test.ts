import { randomBytes } from "node:crypto";
import { z } from "@gatwech/utils/zod";
import { describe, expect, expectTypeOf, it, onTestFinished, vi } from "vitest";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import { getTestInstance } from "../test-utils/test-instance";
import { defineTopics } from "../topics";
import { createKafka } from ".";
import { createCircuitBreaker } from "./circuit-breaker";
import { createReconnectionMonitor } from "./reconnection";

/**
 * The contract is types, so these are checked by `tsc` and never run
 */
describe("client - public types", () => {
	it("types an event from the schema of the topic it names", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string(), email: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});

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
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string(), email: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});

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
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
			const withoutTopics = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
			});

			// @ts-expect-error there is no such topic
			void kafka.publish({ topic: "userDeleted", event: {} });
			void kafka.consume({
				// @ts-expect-error there is no such topic
				topic: "userDeleted",
				handler: async () => {},
			});
			// @ts-expect-error a client with no topics has none to name
			void withoutTopics.publish({ topic: "userRegistered", event: {} });
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
			expectTypeOf(consumer).resolves.toHaveProperty("disconnect");
		};
		void check;
	});

	it("reads a topic with nothing but the topic and a handler", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});

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
				dlq: { attempts: 2 },
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
					userRegistered: {
						name: "user.registered",
						namespaced: false,
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
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
				namespace: "prod",
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
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
	it("should stay closed below the failure threshold", async () => {
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
		});

		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow("broker down");

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(() => Promise.resolve("sent"))).resolves.toBe(
			"sent",
		);
	});

	it("should count only the calls that fail in a row", async () => {
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
		});

		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		await breaker.run(() => Promise.resolve("sent"));
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();

		expect(breaker.state()).toBe("closed");
	});

	it("should open at the threshold and refuse calls without making them", async () => {
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
		});
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		const mockCall = vi.fn(() => Promise.resolve("sent"));

		const refused = await breaker.run(mockCall).catch((error) => error);

		expect(breaker.state()).toBe("open");
		expect(refused).toBeInstanceOf(KafkaError);
		expect(refused.message).toBe(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
		expect(mockCall).not.toHaveBeenCalled();
	});

	it("should let exactly one call through once the reset timeout has passed", async () => {
		let time = 0;
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
			now: () => time,
		});
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		time += 1_000;
		expect(breaker.state()).toBe("half-open");

		let finishTrial!: () => void;
		const trial = breaker.run(
			() =>
				new Promise<string>((resolve) => {
					finishTrial = () => resolve("sent");
				}),
		);
		const mockOther = vi.fn(() => Promise.resolve("sent"));
		const refused = await breaker.run(mockOther).catch((error) => error);

		expect(refused.message).toBe(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
		expect(mockOther).not.toHaveBeenCalled();
		finishTrial();
		await expect(trial).resolves.toBe("sent");
	});

	it("should close when the call it let through works", async () => {
		let time = 0;
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
			now: () => time,
		});
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		time += 1_000;

		await expect(breaker.run(() => Promise.resolve("sent"))).resolves.toBe(
			"sent",
		);

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(() => Promise.resolve("sent"))).resolves.toBe(
			"sent",
		);
	});

	it("should open again when the call it let through fails", async () => {
		let time = 0;
		const breaker = createCircuitBreaker({
			enabled: true,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
			now: () => time,
		});
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow();
		time += 1_000;

		await expect(
			breaker.run(() => Promise.reject(new Error("broker down"))),
		).rejects.toThrow("broker down");

		expect(breaker.state()).toBe("open");
		const mockCall = vi.fn(() => Promise.resolve("sent"));
		await expect(breaker.run(mockCall)).rejects.toThrow(
			KAFKA_ERROR_CODES.CIRCUIT_OPEN,
		);
		expect(mockCall).not.toHaveBeenCalled();
		time += 999;
		expect(breaker.state()).toBe("open");
		time += 1;
		expect(breaker.state()).toBe("half-open");
	});

	it("should only call the function when it is disabled", async () => {
		const breaker = createCircuitBreaker({
			enabled: false,
			failureThreshold: 2,
			resetTimeoutMs: 1_000,
		});

		for (let call = 0; call < 5; call++) {
			await expect(
				breaker.run(() => Promise.reject(new Error("broker down"))),
			).rejects.toThrow("broker down");
		}

		expect(breaker.state()).toBe("closed");
		await expect(breaker.run(() => Promise.resolve("sent"))).resolves.toBe(
			"sent",
		);
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/25
 */
describe("client - reconnection monitor", () => {
	it("should reconnect when the connection is down", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const mockReconnect = vi.fn(async () => {});
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => false,
			reconnect: mockReconnect,
			onError: vi.fn(),
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(mockReconnect).toHaveBeenCalledTimes(1);
		monitor.stop();
	});

	it("should leave a working connection alone", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const mockReconnect = vi.fn(async () => {});
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => true,
			reconnect: mockReconnect,
			onError: vi.fn(),
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(5_000);

		expect(mockReconnect).not.toHaveBeenCalled();
		monitor.stop();
	});

	it("should not start a check or a reconnection while one is running", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		let finishReconnection!: () => void;
		const mockReconnect = vi.fn(
			() =>
				new Promise<void>((resolve) => {
					finishReconnection = resolve;
				}),
		);
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => false,
			reconnect: mockReconnect,
			onError: vi.fn(),
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(5_000);
		expect(mockReconnect).toHaveBeenCalledTimes(1);

		finishReconnection();
		await vi.advanceTimersByTimeAsync(1_000);
		expect(mockReconnect).toHaveBeenCalledTimes(2);
		monitor.stop();
	});

	it("should report what a reconnection throws and try again on the next tick", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const failure = new Error("still down");
		const mockReconnect = vi.fn(async () => {
			throw failure;
		});
		const mockOnError = vi.fn();
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => false,
			reconnect: mockReconnect,
			onError: mockOnError,
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(2_000);

		expect(mockOnError).toHaveBeenCalledWith(failure);
		expect(mockReconnect).toHaveBeenCalledTimes(2);
		monitor.stop();
	});

	it("should report what a check throws", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const failure = new Error("cannot tell");
		const mockReconnect = vi.fn(async () => {});
		const mockOnError = vi.fn();
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => {
				throw failure;
			},
			reconnect: mockReconnect,
			onError: mockOnError,
		});

		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(mockOnError).toHaveBeenCalledWith(failure);
		expect(mockReconnect).not.toHaveBeenCalled();
		monitor.stop();
	});

	it("should stop checking once stopped", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const mockReconnect = vi.fn(async () => {});
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => false,
			reconnect: mockReconnect,
			onError: vi.fn(),
		});
		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		monitor.stop();
		await vi.advanceTimersByTimeAsync(5_000);

		expect(mockReconnect).toHaveBeenCalledTimes(1);
		expect(monitor.isRunning()).toBe(false);
	});

	it("should run one timer however often it is started", async () => {
		vi.useFakeTimers();
		onTestFinished(() => {
			vi.useRealTimers();
		});
		const mockReconnect = vi.fn(async () => {});
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => false,
			reconnect: mockReconnect,
			onError: vi.fn(),
		});

		monitor.start();
		monitor.start();
		await vi.advanceTimersByTimeAsync(1_000);

		expect(mockReconnect).toHaveBeenCalledTimes(1);
		monitor.stop();
	});

	it("should not keep the process alive", () => {
		const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
		onTestFinished(() => setIntervalSpy.mockRestore());
		const monitor = createReconnectionMonitor({
			intervalMs: 1_000,
			check: async () => true,
			reconnect: async () => {},
			onError: vi.fn(),
		});

		monitor.start();

		const timer = setIntervalSpy.mock.results[0]?.value as NodeJS.Timeout;
		expect(timer.hasRef()).toBe(false);
		monitor.stop();
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/24
 */
describe("client - connect", () => {
	it("should connect and be healthy", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);

		await kafka.connect();

		expect(await kafka.healthCheck()).toEqual({
			healthy: true,
			connected: true,
			circuit: "closed",
		});
	});

	it("should make one connection for calls made at once, and for a later one", async () => {
		const { kafka, countLogs, close } = await getTestInstance();
		onTestFinished(close);

		await Promise.all([kafka.connect(), kafka.connect()]);
		await kafka.connect();

		expect(countLogs("Connected to Kafka")).toBe(1);
	});

	it("should open no connection until connect is called", async () => {
		const { countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
		});
		onTestFinished(close);

		await new Promise((resolve) => setTimeout(resolve, 200));

		expect(countLogs("Could not connect to Kafka")).toBe(0);
	});

	it("should throw BROKER_UNREACHABLE after exactly connect.attempts tries", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 3, retryDelayMs: 10 },
		});
		onTestFinished(close);

		const startedAt = Date.now();
		const error = await kafka.connect().catch((reason) => reason);

		expect(error).toBeInstanceOf(KafkaError);
		expect(error.message).toBe(KAFKA_ERROR_CODES.BROKER_UNREACHABLE);
		expect(error.cause).toBeDefined();
		expect(countLogs("Could not connect to Kafka")).toBe(3);
		// kafkajs would retry each of the three by itself, for about 8 seconds
		expect(Date.now() - startedAt).toBeLessThan(3_000);
	}, 30_000);

	it("should give up on a host that can't be found like on any other failure", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["nonexistent.invalid:9092"],
			connect: { attempts: 2, retryDelayMs: 10 },
		});
		onTestFinished(close);

		const error = await kafka.connect().catch((reason) => reason);

		expect(error.message).toBe(KAFKA_ERROR_CODES.BROKER_UNREACHABLE);
		expect(countLogs("Could not connect to Kafka")).toBe(2);
	}, 30_000);

	it("should stop trying when disconnect is called while it connects", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 50, retryDelayMs: 100 },
			reconnection: { enabled: false },
		});
		onTestFinished(close);

		const connecting = kafka.connect();
		await new Promise((resolve) => setTimeout(resolve, 250));
		await kafka.disconnect();

		await expect(connecting).resolves.toBeUndefined();
		expect(countLogs("Could not connect to Kafka")).toBeLessThan(10);
	}, 30_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/25
 */
describe("client - reconnection", () => {
	it("should keep trying in the background after the first connect fails", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 1, retryDelayMs: 10 },
			reconnection: { intervalMs: 100 },
		});
		onTestFinished(close);
		await kafka.connect().catch(() => undefined);
		expect(countLogs("Could not connect to Kafka")).toBe(1);

		await vi.waitFor(
			() =>
				expect(countLogs("Could not connect to Kafka")).toBeGreaterThanOrEqual(3),
			{ timeout: 5_000, interval: 50 },
		);
	});

	it("should stop trying once disconnected", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 1, retryDelayMs: 10 },
			reconnection: { intervalMs: 100 },
		});
		onTestFinished(close);
		await kafka.connect().catch(() => undefined);
		await kafka.disconnect();
		const attempts = countLogs("Could not connect to Kafka");

		await new Promise((resolve) => setTimeout(resolve, 600));

		expect(countLogs("Could not connect to Kafka")).toBe(attempts);
	});

	it("should release its timer once disconnected", async () => {
		const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
		const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");
		onTestFinished(() => {
			setIntervalSpy.mockRestore();
			clearIntervalSpy.mockRestore();
		});
		const { kafka, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 1, retryDelayMs: 10 },
			reconnection: { intervalMs: 123 },
		});
		onTestFinished(close);
		await kafka.connect().catch(() => undefined);
		const monitorCall = setIntervalSpy.mock.calls.findIndex(
			([, intervalMs]) => intervalMs === 123,
		);
		const timer = setIntervalSpy.mock.results[monitorCall]?.value;

		await kafka.disconnect();

		expect(monitorCall).toBeGreaterThanOrEqual(0);
		expect(clearIntervalSpy).toHaveBeenCalledWith(timer);
	});

	it("should not reconnect when reconnection is off", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			brokers: ["localhost:1"],
			connect: { attempts: 1, retryDelayMs: 10 },
			reconnection: { enabled: false, intervalMs: 100 },
		});
		onTestFinished(close);
		await kafka.connect().catch(() => undefined);

		await new Promise((resolve) => setTimeout(resolve, 500));

		expect(countLogs("Could not connect to Kafka")).toBe(1);
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/23
 */
describe("client - provision", () => {
	it("should create each topic with its settings, and a DLQ for each", async () => {
		const { kafka, namespace, describeTopic, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					partitions: 2,
					retentionMs: 60_000,
					cleanupPolicy: "delete",
				},
			}),
		});
		onTestFinished(close);

		await kafka.connect();

		const topic = await describeTopic(`${namespace}.user.registered`);
		const dlq = await describeTopic(`${namespace}.user.registered.dlq`);
		expect(topic).toEqual({
			partitions: 2,
			retentionMs: 60_000,
			cleanupPolicy: "delete",
		});
		expect(dlq).toEqual({
			partitions: 1,
			retentionMs: 60_000,
			cleanupPolicy: "delete",
		});
	}, 30_000);

	it("should keep the declared name of a topic that opts out of the namespace", async () => {
		const { kafka, uniqueName, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userCreated: {
						name: "user.created",
						namespaced: false,
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		const name = uniqueName("user.created");
		// A bare name is shared by every client of the broker, so this one is
		// declared under a name no other test uses
		const bare = createKafka({
			clientId: "payvo-test-bare",
			brokers: kafka.options.brokers,
			namespace,
			topics: defineTopics({
				userCreated: {
					name,
					namespaced: false,
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(() => bare.disconnect());

		await bare.connect();

		expect(await describeTopic(name)).toBeDefined();
		expect(await describeTopic(`${name}.dlq`)).toBeDefined();
		expect(await describeTopic(`${namespace}.${name}`)).toBeUndefined();
	}, 30_000);

	it("should use provisioning for what a topic leaves out", async () => {
		const { kafka, namespace, describeTopic, close } = await getTestInstance({
			provisioning: { partitions: 2 },
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);

		await kafka.connect();

		const topic = await describeTopic(`${namespace}.user.registered`);
		expect(topic?.partitions).toBe(2);
	}, 30_000);

	it("should leave topics that exist alone, and be callable again", async () => {
		const { kafka, brokers, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
						retentionMs: 60_000,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		await kafka.disconnect();
		const again = createKafka({
			clientId: "payvo-test-again",
			brokers,
			namespace,
			topics: kafka.options.topics,
		});
		onTestFinished(() => again.disconnect());

		await again.connect();
		await again.provision();

		const topic = await describeTopic(`${namespace}.user.registered`);
		expect(topic).toMatchObject({ retentionMs: 60_000 });
	}, 30_000);

	it("should refuse to provision before it is connected", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);

		await expect(kafka.provision()).rejects.toThrow(
			KAFKA_ERROR_CODES.NOT_CONNECTED,
		);
	});

	it("should create nothing for a client without topics", async () => {
		const { kafka, namespace, listTopics, close } = await getTestInstance();
		onTestFinished(close);

		await kafka.connect();

		expect(await listTopics(`${namespace}.`)).toEqual([]);
	});
});

describe("client - provision, topics that exist", () => {
	it("should raise the partitions of a topic that has fewer", async () => {
		const { kafka, brokers, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
						partitions: 1,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		expect(
			(await describeTopic(`${namespace}.user.registered`))?.partitions,
		).toBe(1);
		const raised = createKafka({
			clientId: "payvo-test-raised",
			brokers,
			namespace,
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					partitions: 3,
				},
			}),
		});
		onTestFinished(() => raised.disconnect());

		await raised.connect();

		expect(
			(await describeTopic(`${namespace}.user.registered`))?.partitions,
		).toBe(3);
	}, 60_000);

	it("should leave the partitions of a topic that has more", async () => {
		const { kafka, brokers, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
						partitions: 3,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const fewer = createKafka({
			clientId: "payvo-test-fewer",
			brokers,
			namespace,
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					partitions: 1,
				},
			}),
		});
		onTestFinished(() => fewer.disconnect());

		await expect(fewer.connect()).resolves.toBeUndefined();

		expect(
			(await describeTopic(`${namespace}.user.registered`))?.partitions,
		).toBe(3);
	}, 60_000);

	it("should change the retention and the cleanup policy of a topic that differs", async () => {
		const { kafka, brokers, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
						retentionMs: 60_000,
						cleanupPolicy: "delete",
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		const changed = createKafka({
			clientId: "payvo-test-changed",
			brokers,
			namespace,
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					retentionMs: 120_000,
					cleanupPolicy: "compact,delete",
				},
			}),
		});
		onTestFinished(() => changed.disconnect());

		await changed.connect();

		// The broker applies a change of configs a moment after it accepts it
		await vi.waitFor(
			async () =>
				expect(await describeTopic(`${namespace}.user.registered`)).toMatchObject({
					retentionMs: 120_000,
					cleanupPolicy: "compact,delete",
				}),
			{ timeout: 10_000, interval: 200 },
		);
	}, 60_000);

	it("should let six clients provision the same topics at once", async () => {
		const { kafka, brokers, namespace, describeTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		const others = Array.from({ length: 5 }, (_, index) =>
			createKafka({
				clientId: `payvo-test-other-${index}`,
				brokers,
				namespace,
				topics: kafka.options.topics,
			}),
		);
		onTestFinished(async () => {
			await Promise.all(others.map((other) => other.disconnect()));
		});

		await Promise.all([
			kafka.connect(),
			...others.map((other) => other.connect()),
		]);

		expect(await describeTopic(`${namespace}.user.registered`)).toBeDefined();
		expect(await describeTopic(`${namespace}.user.registered.dlq`)).toBeDefined();
	}, 60_000);

	it("should keep the declaration of a DLQ that is declared as a topic", async () => {
		const { kafka, namespace, describeTopic, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
				userRegisteredDlq: {
					name: "user.registered.dlq",
					schema: z.object({ errorMessage: z.string() }),
					key: (event) => event.errorMessage,
					partitions: 2,
					retentionMs: 123_000,
				},
			}),
		});
		onTestFinished(close);

		await kafka.connect();

		expect(await describeTopic(`${namespace}.user.registered.dlq`)).toMatchObject(
			{ partitions: 2, retentionMs: 123_000 },
		);
	}, 60_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/30
 */
describe("client - healthCheck", () => {
	it("should report a client that isn't connected without connecting it", async () => {
		const { kafka, countLogs, close } = await getTestInstance();
		onTestFinished(close);

		const first = await kafka.healthCheck();
		const second = await kafka.healthCheck();

		expect(first).toEqual({
			healthy: false,
			connected: false,
			circuit: "closed",
		});
		expect(second).toEqual(first);
		expect(countLogs("Connected to Kafka")).toBe(0);
	});

	it("should leave the client usable for what comes after it", async () => {
		const { kafka, namespace, describeTopic, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.healthCheck();
		await kafka.healthCheck();
		await kafka.provision();

		expect(await kafka.healthCheck()).toMatchObject({ healthy: true });
		expect(await describeTopic(`${namespace}.user.registered`)).toBeDefined();
	}, 30_000);

	it("should report a client that has disconnected", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);
		await kafka.connect();

		await kafka.disconnect();

		expect(await kafka.healthCheck()).toMatchObject({
			healthy: false,
			connected: false,
		});
	});
});

describe("client - disconnect", () => {
	it("should not throw when it is called twice, or before connect", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);

		await expect(kafka.disconnect()).resolves.toBeUndefined();
		await kafka.connect();
		await expect(kafka.disconnect()).resolves.toBeUndefined();
		await expect(kafka.disconnect()).resolves.toBeUndefined();
	});

	it("should connect again after it disconnected", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);
		await kafka.connect();
		await kafka.disconnect();

		await kafka.connect();

		expect(await kafka.healthCheck()).toMatchObject({ healthy: true });
	});
});

describe("client - enabled", () => {
	it("should neither connect nor create topics when it is off, whatever the brokers", async () => {
		const { kafka, namespace, listTopics, countLogs, close } =
			await getTestInstance({
				enabled: false,
				brokers: ["localhost:1"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);

		await expect(kafka.connect()).resolves.toBeUndefined();
		await expect(kafka.provision()).resolves.toBeUndefined();
		await expect(kafka.disconnect()).resolves.toBeUndefined();

		expect(await kafka.healthCheck()).toEqual({
			healthy: false,
			connected: false,
			circuit: "closed",
		});
		expect(countLogs("Could not connect to Kafka")).toBe(0);
		expect(await listTopics(`${namespace}.`)).toEqual([]);
	}, 30_000);
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/32
 */
describe("client - publish", () => {
	it("should publish an event to its topic", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), email: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", email: "a@b.c" },
		});

		const messages = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(messages).toHaveLength(1);
		expect(JSON.parse(messages[0]?.value ?? "")).toMatchObject({
			userId: "u1",
			email: "a@b.c",
			metadata: { source: kafka.options.clientId },
		});
	}, 30_000);

	it("should key the message by the topic's key", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), email: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", email: "a@b.c" },
		});

		const messages = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(messages[0]?.key).toBe("u1");
	}, 30_000);

	it("should write the headers that consumers read today", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		const body = JSON.parse(message?.value ?? "");
		expect(message?.headers["event-version"]).toBe("1.0");
		expect(message?.headers["event-type"]).toBe(`${namespace}.user.registered`);
		expect(message?.headers.source).toBe(kafka.options.clientId);
		expect(message?.headers["correlation-id"]).toBe(body.metadata.correlationId);
		expect(body.metadata.correlationId).toMatch(/^evt_/);
		expect(new Date(message?.headers.timestamp ?? "").getTime()).not.toBeNaN();
	}, 30_000);

	it("should use the correlation id it is given", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1" },
			correlationId: "request-42",
		});

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(message?.headers["correlation-id"]).toBe("request-42");
		expect(JSON.parse(message?.value ?? "").metadata.correlationId).toBe(
			"request-42",
		);
	}, 30_000);

	it("should add the headers it is given", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1" },
			headers: { "x-request-id": "r1" },
		});

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(message?.headers["x-request-id"]).toBe("r1");
		expect(message?.headers["event-version"]).toBe("1.0");
	}, 30_000);

	it("should send what the schema outputs", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), age: z.coerce.number() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", age: "42" },
		});

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(JSON.parse(message?.value ?? "").age).toBe(42);
	}, 30_000);

	it("should keep the metadata an event carries", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({
						userId: z.string(),
						metadata: z.object({ schemaHash: z.string() }).optional(),
					}),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", metadata: { schemaHash: "abc" } },
		});

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(message?.headers["schema-hash"]).toBe("abc");
		expect(JSON.parse(message?.value ?? "").metadata).toMatchObject({
			schemaHash: "abc",
			source: kafka.options.clientId,
		});
	}, 30_000);

	it("should date the message by the timestamp of the event", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), timestamp: z.number() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publish({
			topic: "userRegistered",
			event: { userId: "u1", timestamp: 1_700_000_000_000 },
		});

		const [message] = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(message?.headers.timestamp).toBe("2023-11-14T22:13:20.000Z");
	}, 30_000);

	it("should refuse an event the schema rejects, and send nothing", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const error = await kafka
			.publish({ topic: "userRegistered", event: { userId: 1 as any } })
			.catch((reason) => reason);

		expect(error).toBeInstanceOf(KafkaError);
		expect(error.message).toBe(KAFKA_ERROR_CODES.EVENT_REJECTED_BY_SCHEMA);
		expect(error.cause).toBeInstanceOf(z.ZodError);
		expect(
			await readMessages(`${namespace}.user.registered`, { within: 1_000 }),
		).toEqual([]);
	}, 30_000);

	it("should refuse a topic the client doesn't have", async () => {
		const { kafka, close } = await getTestInstance();
		onTestFinished(close);
		await kafka.connect();

		const error = await (kafka as any)
			.publish({ topic: "userRegistered", event: {} })
			.catch((reason: unknown) => reason);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_NOT_DECLARED);
		expect(error.cause).toBe("userRegistered");
	});

	it("should refuse to publish before it is connected", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);

		await expect(
			kafka.publish({ topic: "userRegistered", event: { userId: "u1" } }),
		).rejects.toThrow(KAFKA_ERROR_CODES.NOT_CONNECTED);
	});

	it("should send a batch in one request, tied by one correlation id", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await kafka.publishBatch({
			topic: "userRegistered",
			events: [{ userId: "u1" }, { userId: "u2" }, { userId: "u3" }],
		});

		const messages = await readMessages(`${namespace}.user.registered`, {
			within: 1_500,
		});
		expect(messages.map((message) => message.key).sort()).toEqual([
			"u1",
			"u2",
			"u3",
		]);
		const correlationIds = new Set(
			messages.map((message) => message.headers["correlation-id"]),
		);
		expect(correlationIds.size).toBe(1);
	}, 30_000);

	it("should send none of a batch when one event is rejected", async () => {
		const { kafka, namespace, readMessages, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await expect(
			kafka.publishBatch({
				topic: "userRegistered",
				events: [{ userId: "u1" }, { userId: 2 as any }],
			}),
		).rejects.toThrow(KAFKA_ERROR_CODES.EVENT_REJECTED_BY_SCHEMA);

		expect(
			await readMessages(`${namespace}.user.registered`, { within: 1_000 }),
		).toEqual([]);
	}, 30_000);

	it("should send nothing for an empty batch", async () => {
		const { kafka, close } = await getTestInstance({
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await expect(
			kafka.publishBatch({ topic: "userRegistered", events: [] }),
		).resolves.toBeUndefined();
	}, 30_000);

	it("should not create a topic that's gone, and say so", async () => {
		const { kafka, namespace, describeTopic, deleteTopic, close } =
			await getTestInstance({
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});
		onTestFinished(close);
		await kafka.connect();
		await deleteTopic(`${namespace}.user.registered`);

		const error = await kafka
			.publish({ topic: "userRegistered", event: { userId: "u1" } })
			.catch((reason) => reason);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_NOT_PROVISIONED);
		expect(await describeTopic(`${namespace}.user.registered`)).toBeUndefined();
	}, 60_000);

	it("should not count a topic that's gone against the circuit", async () => {
		const { kafka, namespace, deleteTopic, close } = await getTestInstance({
			circuitBreaker: { failureThreshold: 1 },
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();
		await deleteTopic(`${namespace}.user.registered`);

		await kafka
			.publish({ topic: "userRegistered", event: { userId: "u1" } })
			.catch(() => undefined);

		expect(await kafka.healthCheck()).toMatchObject({ circuit: "closed" });
	}, 60_000);

	it("should open the circuit when the broker refuses messages", async () => {
		const { kafka, close } = await getTestInstance({
			compression: "none",
			circuitBreaker: { failureThreshold: 1 },
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string(), body: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		const failed = await kafka
			.publish({
				topic: "userRegistered",
				// The broker takes a message of 1 MB at most
				event: { userId: "u1", body: randomBytes(1_500_000).toString("base64") },
			})
			.catch((reason) => reason);

		expect(failed.message).toBe(KAFKA_ERROR_CODES.PUBLISH_FAILED);
		expect(failed.cause).toBeDefined();
		expect(await kafka.healthCheck()).toMatchObject({ circuit: "open" });
		await expect(
			kafka.publish({
				topic: "userRegistered",
				event: { userId: "u2", body: "small" },
			}),
		).rejects.toThrow(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
	}, 60_000);

	it("should log and send nothing when the client is not enabled", async () => {
		const { kafka, countLogs, close } = await getTestInstance({
			enabled: false,
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});
		onTestFinished(close);
		await kafka.connect();

		await expect(
			kafka.publish({ topic: "userRegistered", event: { userId: "u1" } }),
		).resolves.toBeUndefined();

		expect(countLogs("Kafka is disabled; not publishing")).toBe(1);
	});
});

/**
 * @see https://github.com/gatwech-eleven/gatwech-kafka/issues/59
 */
describe("client - logger tag", () => {
	it("should tag its log lines as Payvo Kafka", async () => {
		const mockWarn = vi
			.spyOn(console, "warn")
			.mockImplementation(() => undefined);
		onTestFinished(() => {
			mockWarn.mockRestore();
		});
		const kafka = createKafka({
			clientId: "payvo-test",
			brokers: ["localhost:1"],
			enabled: false,
			logger: { level: "warn", disableColors: true },
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		expect(mockWarn).toHaveBeenCalledTimes(1);
		expect(String(mockWarn.mock.calls[0]?.[0])).toContain("[Payvo Kafka]:");
	});

	it("should use the tag the caller gives", async () => {
		const mockWarn = vi
			.spyOn(console, "warn")
			.mockImplementation(() => undefined);
		onTestFinished(() => {
			mockWarn.mockRestore();
		});
		const kafka = createKafka({
			clientId: "payvo-test",
			brokers: ["localhost:1"],
			enabled: false,
			logger: { level: "warn", disableColors: true, tag: "wallet-service" },
			topics: defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		});

		await kafka.publish({ topic: "userRegistered", event: { userId: "u1" } });

		expect(mockWarn).toHaveBeenCalledTimes(1);
		expect(String(mockWarn.mock.calls[0]?.[0])).toContain("[wallet-service]:");
	});
});

describe("client - options", () => {
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
