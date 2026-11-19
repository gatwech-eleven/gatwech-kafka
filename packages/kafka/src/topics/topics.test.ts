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
			example: {
				name: "example.created",
				schema: z.object({ id: z.string(), label: z.string() }),
				key: (event) => event.id,
			},
		});

		expect(topics.example).toMatchObject({
			name: "example.created",
			namespaced: true,
			retentionMs: 604_800_000,
			cleanupPolicy: "delete",
			partitions: undefined,
			replicationFactor: undefined,
		});
	});

	it("should keep what a topic sets", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				namespaced: false,
				schema: z.object({ id: z.string(), label: z.string() }),
				key: (event) => event.id,
				partitions: 3,
				replicationFactor: 2,
				retentionMs: 1_000,
				cleanupPolicy: "compact",
			},
		});

		expect(topics.example).toMatchObject({
			namespaced: false,
			partitions: 3,
			replicationFactor: 2,
			retentionMs: 1_000,
			cleanupPolicy: "compact",
		});
	});

	it("should derive the partition key from the event", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				schema: z.object({ id: z.string(), label: z.string() }),
				key: (event) => event.id,
			},
		});

		const key = topics.example.key({ id: "u1", label: "first" });

		expect(key).toBe("u1");
	});

	it("should return topics that cannot be changed", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				schema: z.object({ id: z.string(), label: z.string() }),
				key: (event) => event.id,
			},
		});

		expect(Object.isFrozen(topics)).toBe(true);
		expect(Object.isFrozen(topics.example)).toBe(true);
	});

	it("should refuse a topic with no name", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_NAME_REQUIRED,
				cause: "example",
			}),
		);
	});

	it("should refuse a topic with no schema", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: undefined as any,
					key: () => "key",
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_SCHEMA_REQUIRED,
				cause: "example",
			}),
		);
	});

	it("should refuse a topic with no key function", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: "id" as any,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.TOPIC_KEY_REQUIRED,
				cause: "example",
			}),
		);
	});

	it("should refuse no partitions", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
					partitions: 0,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.INVALID_TOPIC_OPTION,
				cause: "example.partitions",
			}),
		);
	});

	it("should refuse a part of a partition", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
					partitions: 1.5,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.INVALID_TOPIC_OPTION,
				cause: "example.partitions",
			}),
		);
	});

	it("should refuse a negative replication factor", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
					replicationFactor: -1,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.INVALID_TOPIC_OPTION,
				cause: "example.replicationFactor",
			}),
		);
	});

	it("should refuse no retention", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
					retentionMs: 0,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.INVALID_TOPIC_OPTION,
				cause: "example.retentionMs",
			}),
		);
	});

	it("should refuse a retention that is not a number", () => {
		expect(() =>
			defineTopics({
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
					retentionMs: Number.NaN,
				},
			}),
		).toThrow(
			expect.objectContaining({
				message: KAFKA_ERROR_CODES.INVALID_TOPIC_OPTION,
				cause: "example.retentionMs",
			}),
		);
	});

	it("should refuse two topics of one name", () => {
		expect(() =>
			defineTopics({
				registered: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
				},
				registeredAgain: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
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
				name: "example.created",
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
			bare: {
				name: "example.created",
				namespaced: false,
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
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
				example: {
					name: "example.created",
					schema: z.object({ id: z.string() }),
					key: (event) => event.id,
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
			example: {
				name: "example.created",
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const name = resolveTopicName(topics.example, "prod");

		expect(name).toBe("prod.example.created");
	});

	it("should keep the name of a topic that opts out", () => {
		const topics = defineTopics({
			another: {
				name: "another.created",
				namespaced: false,
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const name = resolveTopicName(topics.another, "prod");

		expect(name).toBe("another.created");
	});

	it("should keep every name when there is no namespace", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const withoutNamespace = resolveTopicName(topics.example);
		const withEmptyNamespace = resolveTopicName(topics.example, "");

		expect(withoutNamespace).toBe("example.created");
		expect(withEmptyNamespace).toBe("example.created");
	});

	it("should name the DLQ of a namespaced topic after its name on the broker", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const name = defaultDlqTopicName(topics.example, "prod");

		expect(name).toBe("prod.example.created.dlq");
	});

	it("should name the DLQ of a topic that opts out after its declared name", () => {
		const topics = defineTopics({
			another: {
				name: "another.created",
				namespaced: false,
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const name = defaultDlqTopicName(topics.another, "prod");

		expect(name).toBe("another.created.dlq");
	});

	it("should resolve every topic and leave the declared ones as they were", () => {
		const topics = defineTopics({
			example: {
				name: "example.created",
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
			another: {
				name: "another.created",
				namespaced: false,
				schema: z.object({ id: z.string() }),
				key: (event) => event.id,
			},
		});

		const resolved = resolveTopics(topics, "prod");

		expect(resolved.example.name).toBe("prod.example.created");
		expect(resolved.another.name).toBe("another.created");
		expect(topics.example.name).toBe("example.created");
		expect(Object.isFrozen(resolved)).toBe(true);
		expect(resolved.example.key({ id: "u1" })).toBe("u1");
	});
});
