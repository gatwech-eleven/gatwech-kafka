import { z } from "@gatwech/utils/zod";
import { describe, expect, it } from "vitest";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import {
	defaultDlqTopicName,
	defineTopics,
	resolveTopicName,
	resolveTopics,
} from ".";

const schema = z.object({ userId: z.string(), email: z.string() });
const key = (event: { userId: string }) => event.userId;

const refusal = (declare: () => unknown) => {
	try {
		declare();
	} catch (error) {
		return error as KafkaError;
	}
	throw new Error("defineTopics did not refuse");
};

describe("topics - defineTopics", () => {
	it("should fill in the defaults a topic leaves out", () => {
		const topics = defineTopics({
			userRegistered: { name: "user.registered", schema, key },
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
				schema,
				key,
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
			userRegistered: { name: "user.registered", schema, key },
		});

		expect(topics.userRegistered.key({ userId: "u1", email: "a@b.c" })).toBe(
			"u1",
		);
	});

	it("should return topics that cannot be changed", () => {
		const topics = defineTopics({
			userRegistered: { name: "user.registered", schema, key },
		});

		expect(Object.isFrozen(topics)).toBe(true);
		expect(Object.isFrozen(topics.userRegistered)).toBe(true);
	});

	it("should refuse a topic with no name", () => {
		const error = refusal(() =>
			defineTopics({ userRegistered: { name: "", schema, key } }),
		);

		expect(error).toBeInstanceOf(KafkaError);
		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_NAME_REQUIRED);
		expect(error.cause).toBe("userRegistered");
	});

	it("should refuse a topic with no schema", () => {
		const error = refusal(() =>
			defineTopics({
				userRegistered: { name: "user.registered", schema: undefined as any, key },
			}),
		);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_SCHEMA_REQUIRED);
		expect(error.cause).toBe("userRegistered");
	});

	it("should refuse a topic with no key function", () => {
		const error = refusal(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema,
					key: "userId" as any,
				},
			}),
		);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_KEY_REQUIRED);
		expect(error.cause).toBe("userRegistered");
	});

	it.each([
		["partitions", 0],
		["partitions", 1.5],
		["replicationFactor", -1],
		["retentionMs", 0],
		["retentionMs", Number.NaN],
	] as const)("should refuse %s of %s", (option, value) => {
		const error = refusal(() =>
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema,
					key,
					[option]: value,
				},
			}),
		);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_OPTION_INVALID);
		expect(error.cause).toBe(`userRegistered.${option}`);
	});

	it("should refuse two topics of one name", () => {
		const error = refusal(() =>
			defineTopics({
				registered: { name: "user.registered", schema, key },
				registeredAgain: { name: "user.registered", schema, key },
			}),
		);

		expect(error.message).toBe(KAFKA_ERROR_CODES.TOPIC_NAME_DUPLICATED);
		expect(error.cause).toBe("registeredAgain");
	});

	it("should allow a namespaced and a bare topic of one name", () => {
		const topics = defineTopics({
			namespaced: { name: "user.registered", schema, key },
			bare: { name: "user.registered", namespaced: false, schema, key },
		});

		expect(Object.keys(topics)).toEqual(["namespaced", "bare"]);
	});

	it("should read nothing from the environment", () => {
		const read: string[] = [];
		const env = new Proxy(process.env, {
			get: (target, name) => {
				read.push(String(name));
				return target[name as string];
			},
		});
		const original = process.env;
		process.env = env;
		try {
			defineTopics({ userRegistered: { name: "user.registered", schema, key } });
		} finally {
			process.env = original;
		}

		expect(read).toEqual([]);
	});
});

describe("topics - names on the broker", () => {
	const topics = defineTopics({
		namespaced: { name: "user.registered", schema, key },
		bare: { name: "user.created", namespaced: false, schema, key },
	});

	it("should put the namespace in front of a namespaced topic", () => {
		expect(resolveTopicName(topics.namespaced, "prod")).toBe(
			"prod.user.registered",
		);
	});

	it("should keep the name of a topic that opts out", () => {
		expect(resolveTopicName(topics.bare, "prod")).toBe("user.created");
	});

	it("should keep every name when there is no namespace", () => {
		expect(resolveTopicName(topics.namespaced)).toBe("user.registered");
		expect(resolveTopicName(topics.namespaced, "")).toBe("user.registered");
	});

	it("should name the DLQ after the name on the broker", () => {
		expect(defaultDlqTopicName(topics.namespaced, "prod")).toBe(
			"prod.user.registered.dlq",
		);
		expect(defaultDlqTopicName(topics.bare, "prod")).toBe("user.created.dlq");
	});

	it("should resolve every topic and leave the declared ones as they were", () => {
		const resolved = resolveTopics(topics, "prod");

		expect(resolved.namespaced.name).toBe("prod.user.registered");
		expect(resolved.bare.name).toBe("user.created");
		expect(topics.namespaced.name).toBe("user.registered");
		expect(Object.isFrozen(resolved)).toBe(true);
		expect(resolved.namespaced.key({ userId: "u1", email: "" })).toBe("u1");
	});
});
