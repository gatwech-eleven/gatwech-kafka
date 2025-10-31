import { Admin } from 'kafkajs';
import { T as TopicConfig } from '../topic-config-schema-CWgKTEuX.cjs';
import '@payvo/utils/zod';

/**
 * Options for topic provisioning defaults.
 */
type EnsureOpts = {
    /** Default number of partitions for new topics (overridden by per-topic config if provided). */
    partitions?: number;
    /** Default replication factor for new topics (overridden by per-topic config if provided). */
    replicationFactor?: number;
    /** Max time to wait for leaders after create/partition increase, in milliseconds. */
    waitForLeadersMs?: number;
};
/**
 * Convert the exported `TOPICS` map to normalized topic definitions.
 *
 * @returns Array of normalized {@link TopicDef}.
 */
type TopicMap = Record<string, {
    name: string;
    config: TopicConfig;
}>;
/**
 * Ensure all topics declared in `TOPICS` exist with the desired partition counts and configs.
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
 * @example
 * ```ts
 * import { kafkaClient } from '@/config/kafka';
 * import { ensureTopicsWithConfig } from '@payvo/kafka/ensure-topics';
 *
 * const admin = kafkaClient.getAdmin();
 * await ensureTopicsWithConfig(admin, { partitions: 3, replicationFactor: 1 }); // connects/disconnects admin by default
 * ```
 *
 * @param admin - KafkaJS Admin client.
 * @param opts - Default provisioning options (per-topic overrides take precedence).
 * @param opts.partitions - Default partitions for new topics (default: 3).
 * @param opts.replicationFactor - Default replication factor for new topics (default: 1).
 * @param opts.waitForLeadersMs - Max time to wait for leader election after changes (default: 15000ms).
 * @param manageConnection - If `true`, this function connects/disconnects the Admin (default: `true`).
 * @returns Promise that resolves when topics are ensured.
 * @throws Propagates unexpected admin errors (topic creation races are handled gracefully).
 */
declare function ensureTopicsWithConfig(admin: Admin, opts?: EnsureOpts, manageConnection?: boolean): Promise<void>;

export { type TopicMap, ensureTopicsWithConfig };
