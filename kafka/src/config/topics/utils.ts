import { logger } from "../../utils/logger";
import type { Admin, DescribeConfigResponse } from "kafkajs";
import type { TopicConfig } from "./topic-config-schema";

export const CONFIG_RESOURCE_TYPE_TOPIC = 2;
const RETRY_DELAY_BASE_MS = 250;
const RETRY_DELAY_CAP_MS = 2000;

/**
 * Normalized shape for a topic's desired configuration.
 */
export type TopicConfigShape = {
	/**
	 * Time-based retention (ms). Note: ignored by Kafka when `cleanup.policy` is **only** `"compact"`.
	 * Use `"compact,delete"` to combine compaction with time-based deletion.
	 */
	retentionMs: number;
	/** Cleanup policy: `"delete"`, `"compact"`, or `"compact,delete"`. */
	cleanupPolicy: "delete" | "compact" | "compact,delete";
	/** Desired partition count for this topic (optional; falls back to EnsureOpts.partitions). */
	partitions?: number;
	/** Desired replication factor (optional; falls back to EnsureOpts.replicationFactor). */
	replicationFactor?: number;
};

/**
 * Topic definition (name + desired config) derived from the exported `TOPICS` map.
 */
export type TopicDef = { name: string; config: TopicConfigShape };

export type TopicMap = Record<string, { name: string; config: TopicConfig }>;

export const asTopicDefs = (topics: TopicMap): TopicDef[] =>
	Object.values(topics).map((t) => ({
		name: t.name,
		config: t.config,
	}));

async function sleep(ms: number) {
	return new Promise((r) => setTimeout(r, ms));
}

export async function withRetry<T>(
	fn: () => Promise<T>,
	attempts = 5,
): Promise<T> {
	let i = 0;
	// jittered exponential backoff capped at ~2s
	while (true) {
		try {
			return await fn();
		} catch (e: unknown) {
			i++;
			if (i >= attempts) throw e;
			const delay = Math.min(
				RETRY_DELAY_BASE_MS * 2 ** (i - 1),
				RETRY_DELAY_CAP_MS,
			);
			await sleep(delay);
		}
	}
}

/**
 * Convert a {@link TopicConfigShape} to Kafka config entries suitable for Admin APIs.
 *
 * @param cfg - Topic-level configuration.
 * @returns Array of `{ name, value }` pairs for Kafka topic configs.
 */
export const toConfigEntries = (cfg: TopicConfigShape) => [
	{ name: "retention.ms", value: String(cfg.retentionMs) },
	{ name: "cleanup.policy", value: cfg.cleanupPolicy },
];

/**
 * Validate the topic definitions before applying them to the cluster.
 * - Ensures topic names are present and unique.
 * - Warns when retention is set but policy is `"compact"` (retention will be ignored).
 * - Warns on unexpected `cleanup.policy` values.
 *
 * @param defs - Normalized topic definitions.
 * @throws If duplicate or empty topic names are found.
 */
export function validateTopics(defs: TopicDef[]) {
	const names = new Set<string>();
	for (const t of defs) {
		if (!t.name) throw new Error("TOPICS entry missing name");
		if (names.has(t.name)) throw new Error(`Duplicate topic name: ${t.name}`);
		names.add(t.name);

		if (
			!["delete", "compact", "compact,delete"].includes(t.config.cleanupPolicy)
		) {
			logger.warn(
				"unexpected cleanup.policy value; expected delete|compact|compact,delete",
				{
					topic: t.name,
					cleanupPolicy: t.config.cleanupPolicy,
				},
			);
		}
		if (t.config.cleanupPolicy === "compact" && t.config.retentionMs > 0) {
			logger.warn(
				'retention.ms is set but cleanup.policy is "compact"; retention is ignored unless policy is "compact,delete"',
				{ topic: t.name, retentionMs: t.config.retentionMs },
			);
		}
	}
}

/**
 * Poll until all partitions for a topic have leaders or until a timeout occurs.
 *
 * @param admin - KafkaJS Admin client.
 * @param topic - Topic name to check.
 * @param expectedPartitions - Expected number of partitions that should have leaders.
 * @param timeoutMs - Maximum time to wait for leaders before giving up.
 * @returns Resolves when leaders are present or timeout elapses.
 */
export async function waitForLeaders(
	admin: Admin,
	topic: string,
	expectedPartitions: number,
	timeoutMs: number,
) {
	const deadline = Date.now() + timeoutMs;
	if (expectedPartitions <= 0) return;

	while (true) {
		const md = await admin.fetchTopicMetadata({ topics: [topic] });
		const t = md.topics.find((x) => x.name === topic);
		const leadersOk =
			!!t &&
			t.partitions.length >= expectedPartitions &&
			t.partitions.every((p) => p.leader !== -1);

		if (leadersOk) return;
		if (Date.now() >= deadline) {
			logger.warn("waitForLeaders timeout; leaders may still be electing", {
				topic,
				have: t?.partitions.length,
				expected: expectedPartitions,
			});
			return;
		}
		await new Promise((r) => setTimeout(r, 250));
	}
}

/**
 * Attempt to retrieve current topic configs for a diff; not all clusters/versions/ACLs allow it.
 *
 * @param admin - KafkaJS Admin client.
 * @param topics - Topic names to describe.
 * @returns The DescribeConfigs response, or `undefined` if unsupported/forbidden.
 */
export async function safeDescribeConfigs(
	admin: Admin,
	topicNames: string[],
): Promise<DescribeConfigResponse | undefined> {
	// describeConfigs is not available on all brokers / versions
	const maybe = admin as unknown as {
		describeConfigs?: (args: {
			resources: { type: number; name: string }[];
			includeSynonyms?: boolean;
			includeDocumentation?: boolean;
		}) => Promise<DescribeConfigResponse>;
	};
	if (typeof maybe.describeConfigs !== "function") return undefined;
	try {
		return await maybe.describeConfigs({
			resources: topicNames.map((name) => ({
				type: CONFIG_RESOURCE_TYPE_TOPIC,
				name,
			})),
			includeSynonyms: false,
			includeDocumentation: false,
		});
	} catch {
		return undefined;
	}
}
