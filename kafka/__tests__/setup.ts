import type { Admin } from "kafkajs";
import { vi } from "vitest";

declare global {
	// eslint-disable-next-line no-var
	var __appMocks: {
		makeAdminMock: (opts?: {
			partitionsAfterCreate?: number;
			hasDescribeConfigs?: boolean;
			describeForTopics?: Record<
				string,
				{ retentionMs?: string; cleanupPolicy?: string }
			>;
		}) => {
			admin: Partial<Admin>;
			calls: {
				createTopics: Parameters<Admin["createTopics"]>[0][];
				alterConfigs: Parameters<Admin["alterConfigs"]>[0][];
				createPartitions: Parameters<Admin["createPartitions"]>[0][];
			};
		};
	};
}

vi.useFakeTimers();

// Provide a reusable Admin mock factory for tests
globalThis.__appMocks = {
	makeAdminMock(opts = {}) {
		const {
			partitionsAfterCreate = 3,
			hasDescribeConfigs = false,
			describeForTopics = {},
		} = opts;
		let partitionCount = partitionsAfterCreate;

		const calls: {
			createTopics: Parameters<Admin["createTopics"]>[0][];
			alterConfigs: Parameters<Admin["alterConfigs"]>[0][];
			createPartitions: Parameters<Admin["createPartitions"]>[0][];
		} = {
			createTopics: [],
			alterConfigs: [],
			createPartitions: [],
		};
		let created = false;
		const createdTopicNames = new Set<string>();

		const admin: Partial<Admin> = {
			connect: vi.fn().mockResolvedValue(undefined),
			disconnect: vi.fn().mockResolvedValue(undefined),
			fetchTopicMetadata: vi
				.fn()
				.mockImplementation(async ({ topics }: { topics?: string[] } = {}) => {
					if (!created) return { topics: [] };
					const topicList =
						Array.isArray(topics) && topics.length
							? topics
							: Array.from(createdTopicNames);
					return {
						topics: topicList.map((name: string) => ({
							name,
							partitions: Array.from({ length: partitionCount }).map(
								(_: unknown, i: number) => ({ partitionId: i, leader: 1 }),
							),
						})),
					};
				}),
			createTopics: vi
				.fn()
				.mockImplementation(async (arg: Parameters<Admin["createTopics"]>[0]) => {
					calls.createTopics.push(arg);
					created = true;
					try {
						for (const t of arg.topics || []) {
							if (t?.topic) createdTopicNames.add(t.topic);
						}
					} catch {}
					return true;
				}),
			alterConfigs: vi
				.fn()
				.mockImplementation(async (arg: Parameters<Admin["alterConfigs"]>[0]) => {
					calls.alterConfigs.push(arg);
					return undefined;
				}),
			createPartitions: vi
				.fn()
				.mockImplementation(
					async (arg: Parameters<Admin["createPartitions"]>[0]) => {
						calls.createPartitions.push(arg);
						try {
							const tp = (arg.topicPartitions || [])[0];
							if (tp?.count) {
								// simulate broker now reporting the new partition count
								partitionCount = tp.count;
							}
						} catch {}
						return undefined;
					},
				),
		};

		if (hasDescribeConfigs) {
			admin.describeConfigs = vi
				.fn()
				.mockImplementation(
					async ({ resources }: { resources: { name: string }[] }) => {
						type DescribeConfigsReturn = Awaited<
							ReturnType<NonNullable<Admin["describeConfigs"]>>
						>;
						const result = {
							throttleTime: 0,
							resources: resources.map((r) => {
								const cfg = describeForTopics[r.name] || {};
								return {
									resourceType: 2,
									resourceName: r.name,
									configEntries: [
										{
											configName: "retention.ms",
											configValue: cfg.retentionMs ?? "604800000",
											isDefault: true,
											configSource: 0,
											isSensitive: false,
											readOnly: false,
											configSynonyms: [],
										},
										{
											configName: "cleanup.policy",
											configValue: cfg.cleanupPolicy ?? "delete",
											isDefault: true,
											configSource: 0,
											isSensitive: false,
											readOnly: false,
											configSynonyms: [],
										},
									],
									errorCode: 0,
									errorMessage: "",
								};
							}),
						} satisfies Omit<DescribeConfigsReturn, "throttleTime"> & {
							throttleTime: number;
						} as DescribeConfigsReturn;
						return result as DescribeConfigsReturn;
					},
				);
		}

		return { admin, calls };
	},
};
