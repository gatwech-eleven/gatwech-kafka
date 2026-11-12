import type { Admin } from "kafkajs";
import type {
	TopicConfig,
	ValidatedTopicConfig,
} from "./topics/topic-config-schema";
import { createMissingTopics } from "./topics/create-topics";
import { updateTopicConfigs } from "./topics/update-configs";
import { updatePartitionCounts } from "./topics/update-partitions";
import { asTopicDefs, withRetry, validateTopics } from "./topics/utils";
import type { TopicMap } from "./topics/utils";

const WAIT_FOR_LEADERS_DEFAULT_MS = 15_000;

/**
 * Options for topic provisioning defaults.
 */
type EnsureOpts = {
	/**
	 * Application-defined topics (from `createTopicConfig()`).
	 * This is the recommended way to provide topics.
	 */
	topics?: Record<string, ValidatedTopicConfig | TopicConfig>;
	/** Default number of partitions for new topics (overridden by per-topic config if provided). */
	partitions?: number;
	/** Default replication factor for new topics (overridden by per-topic config if provided). */
	replicationFactor?: number;
	/** Max time to wait for leaders after create/partition increase, in milliseconds. */
	waitForLeadersMs?: number;
};

/**
 * Initialize Kafka topics with application-level configurations.
 *
 * **What this does:**
 * 1. **Create** any missing topics with initial `numPartitions`, `replicationFactor`, and `configEntries`.
 * 2. **Increase** partitions when current count is lower than desired (Kafka only allows increasing).
 * 3. **Apply/patch** topic configs (e.g., `retention.ms`, `cleanup.policy`) on both existing and newly created topics.
 *
 * **Notes & caveats:**
 * - To combine compaction **and** time-based deletion, set `cleanup.policy` to `"compact,delete"`.
 * - `alterConfigs` and `describeConfigs` may require admin ACLs; failures are logged as warnings and do not stop startup.
 * - By default this function manages the Admin connection; pass `manageConnection=false` if the caller handles connect/disconnect.
 *
 * @example Application-level topics
 * ```ts
 * import { kafkaClient } from '@/config/kafka';
 * import { initializeKafkaTopics, createTopicConfig } from '@gatwech/kafka';
 *
 * const myTopics = createTopicConfig({
 *   USER_REGISTERED: {
 *     name: 'user.registered',
 *     schema: UserRegisteredSchema,
 *     retentionMs: 7 * 24 * 60 * 60 * 1000,
 *   },
 * });
 *
 * const admin = kafkaClient.getAdmin();
 * await initializeKafkaTopics(admin, {
 *   topics: myTopics,
 *   partitions: 3,
 *   replicationFactor: 1,
 * });
 * ```
 *
 * @param admin - KafkaJS Admin client.
 * @param opts - Initialization options.
 * @param opts.topics - Application-defined topics (from `createTopicConfig()`).
 * @param opts.partitions - Default partitions for new topics (default: 3).
 * @param opts.replicationFactor - Default replication factor for new topics (default: 1).
 * @param opts.waitForLeadersMs - Max time to wait for leader election after changes (default: 15000ms).
 * @param manageConnection - If `true`, this function connects/disconnects the Admin (default: `true`).
 * @returns Promise that resolves when topics are initialized.
 * @throws Propagates unexpected admin errors (topic creation races are handled gracefully).
 */
export async function initializeKafkaTopics(
	admin: Admin,
	opts: EnsureOpts = {},
	manageConnection = true,
) {
	const partitionsDefault = opts.partitions ?? 3;
	const rfDefault = opts.replicationFactor ?? 1;
	const waitForLeadersMs = opts.waitForLeadersMs ?? WAIT_FOR_LEADERS_DEFAULT_MS;

	// Convert provided topics to TopicMap format
	const flatTopics: TopicMap = {};
	if (opts.topics) {
		for (const [key, value] of Object.entries(opts.topics)) {
			flatTopics[key] = { name: value.name, config: value };
		}
	}

	const defs = asTopicDefs(flatTopics);
	validateTopics(defs);

	if (manageConnection) await admin.connect();
	try {
		const md = await withRetry(() => admin.fetchTopicMetadata());
		const existing = new Set(md.topics.map((t) => t.name));

		// Step 1: Create missing topics
		await createMissingTopics(
			admin,
			defs,
			existing,
			partitionsDefault,
			rfDefault,
			waitForLeadersMs,
		);

		// Step 2: Update partition counts
		const md2 = await withRetry(() => admin.fetchTopicMetadata());
		const current = new Map(md2.topics.map((t) => [t.name, t]));
		await updatePartitionCounts(
			admin,
			defs,
			current,
			partitionsDefault,
			waitForLeadersMs,
		);

		// Step 3: Update topic configs
		await updateTopicConfigs(admin, defs);
	} finally {
		if (manageConnection) await admin.disconnect();
	}
}
