import type { Admin } from "kafkajs";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { initializeKafkaTopics } from "../../src/config/initialize.topics";
import { z } from "@gatwech/utils/zod";

describe("initializeKafkaTopics - topics provisioning", () => {
	beforeEach(() => {
		vi.useRealTimers();
	});

	it("ensures only the provided dynamic topic", async () => {
		const { admin, calls } = globalThis.__appMocks.makeAdminMock();

		const topicName = "dynamic.topic";
		await initializeKafkaTopics(admin as Admin, {
			partitions: 3,
			replicationFactor: 1,
			topics: {
				DYNAMIC_TOPIC: {
					name: topicName,
					retentionMs: 86_400_000,
					cleanupPolicy: "delete",
					schema: z.unknown(),
				},
			},
		});

		expect(admin.connect).toHaveBeenCalledTimes(1);
		expect(admin.disconnect).toHaveBeenCalledTimes(1);

		expect(calls.createTopics).toHaveLength(1);
		const createArg = calls.createTopics[0];
		expect(Array.isArray(createArg.topics)).toBe(true);
		expect(createArg.topics).toHaveLength(1);
		expect(createArg.topics[0].topic).toBe(topicName);
		// partitions default propagated
		expect(createArg.topics[0].numPartitions).toBe(3);

		// alterConfigs should target only our topic
		expect(calls.alterConfigs).toHaveLength(1);
		const alterArg = calls.alterConfigs[0];
		const names = alterArg.resources.map((r: any) => r.name);
		expect(names).toEqual([topicName]);

		// no partition increase needed since metadata returns 3 already
		expect(calls.createPartitions).toHaveLength(0);
	}, 20_000);
});

describe("initializeKafkaTopics - behaviors", () => {
	beforeEach(() => {
		vi.useRealTimers();
	});

	it("increases partitions when desired is higher than current", async () => {
		const { admin, calls } = globalThis.__appMocks.makeAdminMock({
			partitionsAfterCreate: 1,
		});

		const topicName = "dynamic.topic";
		await initializeKafkaTopics(admin as Admin, {
			partitions: 3,
			replicationFactor: 1,
			topics: {
				DYNAMIC_TOPIC: {
					name: topicName,
					retentionMs: 86_400_000,
					cleanupPolicy: "delete",
					schema: z.unknown(),
				},
			},
		});

		expect(calls.createTopics).toHaveLength(1);
		expect(calls.createPartitions).toHaveLength(1);
		const incArg = calls.createPartitions[0];
		expect(incArg.topicPartitions).toEqual([{ topic: topicName, count: 3 }]);
	}, 20_000);

	it("diff-based alterConfigs patches only changed keys when describeConfigs available", async () => {
		const topicName = "dynamic.topic";
		const { admin, calls } = globalThis.__appMocks.makeAdminMock({
			hasDescribeConfigs: true,
			describeForTopics: {
				[topicName]: {
					retentionMs: "604800000",
					cleanupPolicy: "delete",
				},
			},
		});

		await initializeKafkaTopics(admin as Admin, {
			partitions: 3,
			replicationFactor: 1,
			topics: {
				DYNAMIC_TOPIC: {
					name: topicName,
					retentionMs: 86_400_000,
					cleanupPolicy: "delete",
					schema: z.unknown(),
				},
			},
		});

		expect(calls.alterConfigs).toHaveLength(1);
		const alterArg = calls.alterConfigs[0];
		expect(alterArg.resources).toHaveLength(1);
		expect(alterArg.resources[0].name).toBe(topicName);
		expect(alterArg.resources[0].configEntries).toEqual([
			{ name: "retention.ms", value: "86400000" },
		]);
	}, 20_000);
});
