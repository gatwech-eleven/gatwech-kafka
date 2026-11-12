import { z } from "@gatwech/utils/zod";
import { describe, expect, it } from "vitest";
import { KAFKA_ERROR_CODES } from "../error-codes";
import {
	defaultDlqTopicName,
	defineTopics,
	resolveTopicName,
	resolveTopics,
} from ".";

describe("topics - defineTopics", () => {
	it("should fill in the defaults a topic leaves out", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string(), email: z.string() }),
				key: (event) => event.userId,
			},
		});

		expect(topics.userRegistered).toMatchObject({
			name: "user.registered",
			namespaced: true,
			retentionMs: 604_800_000,
			cleanupPolicy: "delete",
			partitions: undefined,
			replicationFactor: undefined,
		});
	});

	it("should keep what a topic sets", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				namespaced: false,
				schema: z.object({ userId: z.string(), email: z.string() }),
				key: (event) => event.userId,
				partitions: 3,
				replicationFactor: 2,
				retentionMs: 1_000,
				cleanupPolicy: "compact",
			},
		});

		expect(topics.userRegistered).toMatchObject({
			namespaced: false,
			partitions: 3,
			replicationFactor: 2,
			retentionMs: 1_000,
			cleanupPolicy: "compact",
		});
	});

	it("should derive the partition key from the event", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string(), email: z.string() }),
				key: (event) => event.userId,
			},
		});

		const key = topics.userRegistered.key({ userId: "u1", email: "a@b.c" });

		expect(key).toBe("u1");
	});

	it("should return topics that cannot be changed", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string(), email: z.string() }),
				key: (event) => event.userId,
			},
		});

		expect(Object.isFrozen(topics)).toBe(true);
		expect(Object.isFrozen(topics.userRegistered)).toBe(true);
	});

	it("should refuse a topic with no name", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_NAME_REQUIRED,
				cause: "userRegistered",
			}),
		);
	});

	it("should refuse a topic with no schema", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: undefined as any,
					key: () => "key",
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_SCHEMA_REQUIRED,
				cause: "userRegistered",
			}),
		);
	});

	it("should refuse a topic with no key function", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: "userId" as any,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_KEY_REQUIRED,
				cause: "userRegistered",
			}),
		);
	});

	it("should refuse no partitions", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					partitions: 0,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
				cause: "userRegistered.partitions",
			}),
		);
	});

	it("should refuse a part of a partition", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					partitions: 1.5,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
				cause: "userRegistered.partitions",
			}),
		);
	});

	it("should refuse a negative replication factor", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					replicationFactor: -1,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
				cause: "userRegistered.replicationFactor",
			}),
		);
	});

	it("should refuse no retention", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					retentionMs: 0,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
				cause: "userRegistered.retentionMs",
			}),
		);
	});

	it("should refuse a retention that is not a number", () => {
		expect(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
					retentionMs: Number.NaN,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID,
				cause: "userRegistered.retentionMs",
			}),
		);
	});

	it("should refuse two topics of one name", () => {
		expect(() =>
			defineTopics({
				registered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
				registeredAgain: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_NAME_DUPLICATED,
				cause: "registeredAgain",
			}),
		);
	});

	it("should allow a namespaced and a bare topic of one name", () => {
		const topics = defineTopics({
			namespaced: {
				name: "user.registered",
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
			bare: {
				name: "user.registered",
				namespaced: false,
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		expect(Object.keys(topics)).toEqual(["namespaced", "bare"]);
	});

	it("should read nothing from the environment", () => {
		const read: string[] = [];
		const original = process.env;
		process.env = new Proxy(original, {
			get: (target, name) => {
				read.push(String(name));
				return target[name as string];
			},
		});

		try {
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					key: (event) => event.userId,
				},
			});
		} finally {
			process.env = original;
		}

		expect(read).toEqual([]);
	});
});

describe("topics - names on the broker", () => {
	it("should put the namespace in front of a namespaced topic", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const name = resolveTopicName(topics.userRegistered, "prod");

		expect(name).toBe("prod.user.registered");
	});

	it("should keep the name of a topic that opts out", () => {
		const topics = defineTopics({
			userCreated: {
				name: "user.created",
				namespaced: false,
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const name = resolveTopicName(topics.userCreated, "prod");

		expect(name).toBe("user.created");
	});

	it("should keep every name when there is no namespace", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const withoutNamespace = resolveTopicName(topics.userRegistered);
		const withEmptyNamespace = resolveTopicName(topics.userRegistered, "");

		expect(withoutNamespace).toBe("user.registered");
		expect(withEmptyNamespace).toBe("user.registered");
	});

	it("should name the DLQ of a namespaced topic after its name on the broker", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const name = defaultDlqTopicName(topics.userRegistered, "prod");

		expect(name).toBe("prod.user.registered.dlq");
	});

	it("should name the DLQ of a topic that opts out after its declared name", () => {
		const topics = defineTopics({
			userCreated: {
				name: "user.created",
				namespaced: false,
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const name = defaultDlqTopicName(topics.userCreated, "prod");

		expect(name).toBe("user.created.dlq");
	});

	it("should resolve every topic and leave the declared ones as they were", () => {
		const topics = defineTopics({
			userRegistered: {
				name: "user.registered",
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
			userCreated: {
				name: "user.created",
				namespaced: false,
				schema: z.object({ userId: z.string() }),
				key: (event) => event.userId,
			},
		});

		const resolved = resolveTopics(topics, "prod");

		expect(resolved.userRegistered.name).toBe("prod.user.registered");
		expect(resolved.userCreated.name).toBe("user.created");
		expect(topics.userRegistered.name).toBe("user.registered");
		expect(Object.isFrozen(resolved)).toBe(true);
		expect(resolved.userRegistered.key({ userId: "u1" })).toBe("u1");
	});
});
