const CONFIG_RESOURCE_TYPE_TOPIC = 2;
import { logger } from "../utils/logger";
import type { Admin, DescribeConfigResponse } from "kafkajs";
import type {
  TopicConfig,
  ValidatedTopicConfig,
} from "./topics/topic-config-schema";

const RETRY_DELAY_BASE_MS = 250;
const RETRY_DELAY_CAP_MS = 2000;
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
 * Normalized shape for a topic's desired configuration.
 */
type TopicConfigShape = {
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
type TopicDef = { name: string; config: TopicConfigShape };

/**
 * Convert the exported `TOPICS` map to normalized topic definitions.
 *
 * @returns Array of normalized {@link TopicDef}.
 */

export type TopicMap = Record<string, { name: string; config: TopicConfig }>;

const asTopicDefs = (topics: TopicMap): TopicDef[] =>
  Object.values(topics).map((t) => ({
    name: t.name,
    config: t.config,
  }));

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry<T>(fn: () => Promise<T>, attempts = 5): Promise<T> {
  let i = 0;
  // jittered exponential backoff capped at ~2s
  while (true) {
    try {
      return await fn();
    } catch (e: any) {
      i++;
      if (i >= attempts) throw e;
      const delay = Math.min(RETRY_DELAY_BASE_MS * 2 ** (i - 1), RETRY_DELAY_CAP_MS);
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
const toConfigEntries = (cfg: TopicConfigShape) => [
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
function validateTopics(defs: TopicDef[]) {
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
        }
      );
    }
    if (t.config.cleanupPolicy === "compact" && t.config.retentionMs > 0) {
      logger.warn(
        'retention.ms is set but cleanup.policy is "compact"; retention is ignored unless policy is "compact,delete"',
        { topic: t.name, retentionMs: t.config.retentionMs }
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
async function waitForLeaders(
  admin: Admin,
  topic: string,
  expectedPartitions: number,
  timeoutMs: number
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
async function safeDescribeConfigs(
  admin: Admin,
  topicNames: string[]
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
  manageConnection = true
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

    const createList = defs
      .filter((t) => !existing.has(t.name))
      .map((t) => ({
        topic: t.name,
        numPartitions: t.config.partitions ?? partitionsDefault,
        replicationFactor: t.config.replicationFactor ?? rfDefault,
        configEntries: toConfigEntries(t.config),
      }));

    if (createList.length) {
      logger.info("creating Kafka topics", {
        topics: createList.map((t) => t.topic),
      });
      try {
        await withRetry(() =>
          admin.createTopics({ topics: createList, waitForLeaders: true })
        );
      } catch (e: any) {
        // Race between services; ignore benign "already exists"
        if (!/already exists/i.test(e?.message || "")) throw e;
      }
      // Extra wait to be safe on some clusters
      for (const t of createList) {
        await waitForLeaders(
          admin,
          t.topic,
          t.numPartitions ?? partitionsDefault,
          waitForLeadersMs
        );
      }
    }


    const md2 = await withRetry(() => admin.fetchTopicMetadata());
    const current = new Map(md2.topics.map((t) => [t.name, t]));
    const partitionsToIncrease = defs
      .map((t) => {
        const desired = t.config.partitions ?? partitionsDefault;
        const curCount = current.get(t.name)?.partitions?.length ?? 0;
        return desired > curCount ? { topic: t.name, count: desired } : null;
      })
      .filter(Boolean) as { topic: string; count: number }[];

    if (partitionsToIncrease.length) {
      logger.info("increasing partitions", partitionsToIncrease);
      try {
        await withRetry(() =>
          admin.createPartitions({ topicPartitions: partitionsToIncrease })
        );
      } catch (e: any) {
        // Some brokers may throw if already increased by another service; log and continue
        logger.warn("createPartitions returned error; continuing", {
          error: e?.message,
        });
      }
      for (const t of partitionsToIncrease) {
        await waitForLeaders(admin, t.topic, t.count, waitForLeadersMs);
      }
    }


    const topicNames = defs.map((t) => t.name);
    const desiredByTopic = new Map(
      defs.map((t) => [t.name, toConfigEntries(t.config)])
    );

    // Try to diff with describeConfigs (if supported & allowed)
    const desc = await safeDescribeConfigs(admin, topicNames);
    type AlterResource = {
      type: typeof CONFIG_RESOURCE_TYPE_TOPIC;
      name: string;
      configEntries: { name: string; value: string }[];
    };
    let resources: AlterResource[];

    if (desc?.resources?.length) {
      const currentByTopic = new Map(
        desc.resources.map((r) => [
          r.resourceName,
          new Map(
            (r.configEntries || []).map((c) => [
              (c as any).configName,
              String((c as any).configValue ?? ""),
            ])
          ),
        ])
      );

      const pending: AlterResource[] = [];
      for (const name of topicNames) {
        const desired = desiredByTopic.get(name)!;
        const currentCfg = currentByTopic.get(name);
        const diff = desired.filter(
          (entry) => currentCfg?.get(entry.name) !== entry.value
        );
        if (diff.length)
          pending.push({
            type: CONFIG_RESOURCE_TYPE_TOPIC,
            name,
            configEntries: diff,
          });
      }
      resources = pending;
    } else {
      // Fallback: set everything (may require ACLs)
      resources = topicNames.map((name) => ({
        type: CONFIG_RESOURCE_TYPE_TOPIC,
        name,
        configEntries: desiredByTopic.get(name)!,
      }));
    }

    if (resources.length) {
      try {
        await admin.alterConfigs({ resources, validateOnly: false });
        logger.info("topic configs ensured", {
          topics: resources.map((r) => r.name),
        });
      } catch (e: any) {
        logger.warn(
          "alterConfigs failed (check ACLs / broker version). Topics still created.",
          {
            error: e?.message,
          }
        );
      }
    } else {
      logger.debug("topic configs already up-to-date");
    }
  } finally {
    if (manageConnection) await admin.disconnect();
  }
}

