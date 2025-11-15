import { t as logger } from "./logger-BCcyae9U.mjs";
import { t as topics } from "./topics-DSardRd6.mjs";

//#region src/config/ensure-topics.ts
const CONFIG_RESOURCE_TYPE_TOPIC = 2;
const asTopicDefs = (topics$1) => Object.values(topics$1).map((t) => ({
	name: t.name,
	config: t.config
}));
/**
* Convert a {@link TopicConfigShape} to Kafka config entries suitable for Admin APIs.
*
* @param cfg - Topic-level configuration.
* @returns Array of `{ name, value }` pairs for Kafka topic configs.
*/
const toConfigEntries = (cfg) => [{
	name: "retention.ms",
	value: String(cfg.retentionMs)
}, {
	name: "cleanup.policy",
	value: cfg.cleanupPolicy
}];
/**
* Validate the topic definitions before applying them to the cluster.
* - Ensures topic names are present and unique.
* - Warns when retention is set but policy is `"compact"` (retention will be ignored).
* - Warns on unexpected `cleanup.policy` values.
*
* @param defs - Normalized topic definitions.
* @throws If duplicate or empty topic names are found.
*/
function validateTopics(defs) {
	const names = /* @__PURE__ */ new Set();
	for (const t of defs) {
		if (!t.name) throw new Error("TOPICS entry missing name");
		if (names.has(t.name)) throw new Error(`Duplicate topic name: ${t.name}`);
		names.add(t.name);
		if (![
			"delete",
			"compact",
			"compact,delete"
		].includes(t.config.cleanupPolicy)) logger.warn("unexpected cleanup.policy value; expected delete|compact|compact,delete", {
			topic: t.name,
			cleanupPolicy: t.config.cleanupPolicy
		});
		if (t.config.cleanupPolicy === "compact" && t.config.retentionMs > 0) logger.warn("retention.ms is set but cleanup.policy is \"compact\"; retention is ignored unless policy is \"compact,delete\"", {
			topic: t.name,
			retentionMs: t.config.retentionMs
		});
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
async function waitForLeaders(admin, topic, expectedPartitions, timeoutMs) {
	const deadline = Date.now() + timeoutMs;
	if (expectedPartitions <= 0) return;
	while (true) {
		const t = (await admin.fetchTopicMetadata({ topics: [topic] })).topics.find((x) => x.name === topic);
		if (!!t && t.partitions.length >= expectedPartitions && t.partitions.every((p) => p.leader !== -1)) return;
		if (Date.now() >= deadline) {
			logger.warn("waitForLeaders timeout; leaders may still be electing", {
				topic,
				have: t?.partitions.length,
				expected: expectedPartitions
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
async function safeDescribeConfigs(admin, topics$1) {
	try {
		return await admin.describeConfigs?.({
			resources: topics$1.map((name) => ({
				type: CONFIG_RESOURCE_TYPE_TOPIC,
				name
			})),
			includeSynonyms: false,
			includeDocumentation: false
		});
	} catch {
		return;
	}
}
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
async function ensureTopicsWithConfig(admin, opts = {}, manageConnection = true) {
	const partitionsDefault = opts.partitions ?? 3;
	const rfDefault = opts.replicationFactor ?? 1;
	const waitForLeadersMs = opts.waitForLeadersMs ?? 15e3;
	const defs = asTopicDefs(Object.fromEntries(Object.values(topics).flatMap((group) => Object.values(group).map((t) => [t.name, {
		name: t.name,
		config: t
	}]))));
	validateTopics(defs);
	if (manageConnection) await admin.connect();
	try {
		const md = await admin.fetchTopicMetadata();
		const existing = new Set(md.topics.map((t) => t.name));
		const createList = defs.filter((t) => !existing.has(t.name)).map((t) => ({
			topic: t.name,
			numPartitions: t.config.partitions ?? partitionsDefault,
			replicationFactor: t.config.replicationFactor ?? rfDefault,
			configEntries: toConfigEntries(t.config)
		}));
		if (createList.length) {
			logger.info("creating Kafka topics", { topics: createList.map((t) => t.topic) });
			try {
				await admin.createTopics({
					topics: createList,
					waitForLeaders: true
				});
			} catch (e) {
				if (!/already exists/i.test(e?.message || "")) throw e;
			}
			for (const t of createList) await waitForLeaders(admin, t.topic, t.numPartitions ?? partitionsDefault, waitForLeadersMs);
		}
		const md2 = await admin.fetchTopicMetadata();
		const current = new Map(md2.topics.map((t) => [t.name, t]));
		const partitionsToIncrease = defs.map((t) => {
			const desired = t.config.partitions ?? partitionsDefault;
			return desired > (current.get(t.name)?.partitions?.length ?? 0) ? {
				topic: t.name,
				count: desired
			} : null;
		}).filter(Boolean);
		if (partitionsToIncrease.length) {
			logger.info("increasing partitions", partitionsToIncrease);
			try {
				await admin.createPartitions({ topicPartitions: partitionsToIncrease });
			} catch (e) {
				logger.warn("createPartitions returned error; continuing", { error: e?.message });
			}
			for (const t of partitionsToIncrease) await waitForLeaders(admin, t.topic, t.count, waitForLeadersMs);
		}
		const topics$1 = defs.map((t) => t.name);
		const desiredByTopic = new Map(defs.map((t) => [t.name, toConfigEntries(t.config)]));
		const desc = await safeDescribeConfigs(admin, topics$1);
		let resources;
		if (desc?.resources?.length) {
			const currentByTopic = new Map(desc.resources.map((r) => [r.resourceName, new Map((r.configs || []).map((c) => [c.name, c.value]))]));
			const pending = [];
			for (const name of topics$1) {
				const desired = desiredByTopic.get(name);
				const currentCfg = currentByTopic.get(name);
				const diff = desired.filter((entry) => currentCfg?.get(entry.name) !== entry.value);
				if (diff.length) pending.push({
					type: CONFIG_RESOURCE_TYPE_TOPIC,
					name,
					configEntries: diff
				});
			}
			resources = pending;
		} else resources = topics$1.map((name) => ({
			type: CONFIG_RESOURCE_TYPE_TOPIC,
			name,
			configEntries: desiredByTopic.get(name)
		}));
		if (resources.length) try {
			await admin.alterConfigs({
				resources,
				validateOnly: false
			});
			logger.info("topic configs ensured", { topics: resources.map((r) => r.name) });
		} catch (e) {
			logger.warn("alterConfigs failed (check ACLs / broker version). Topics still created.", { error: e?.message });
		}
		else logger.debug?.("topic configs already up-to-date");
	} finally {
		if (manageConnection) await admin.disconnect();
	}
}

//#endregion
export { ensureTopicsWithConfig as t };