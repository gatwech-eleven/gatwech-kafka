import { l as logger } from '../shared/kafka.Dv3r60xd.mjs';
import { topics } from './topics.mjs';
import 'winston';
import '@payvo/utils/zod';

const CONFIG_RESOURCE_TYPE_TOPIC = 2;
const asTopicDefs = (topics2) => Object.values(topics2).map((t) => ({
  name: t.name,
  config: t.config
}));
const toConfigEntries = (cfg) => [
  { name: "retention.ms", value: String(cfg.retentionMs) },
  { name: "cleanup.policy", value: cfg.cleanupPolicy }
];
function validateTopics(defs) {
  const names = /* @__PURE__ */ new Set();
  for (const t of defs) {
    if (!t.name) throw new Error("TOPICS entry missing name");
    if (names.has(t.name)) throw new Error(`Duplicate topic name: ${t.name}`);
    names.add(t.name);
    if (!["delete", "compact", "compact,delete"].includes(t.config.cleanupPolicy)) {
      logger.warn(
        "unexpected cleanup.policy value; expected delete|compact|compact,delete",
        {
          topic: t.name,
          cleanupPolicy: t.config.cleanupPolicy
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
async function waitForLeaders(admin, topic, expectedPartitions, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  if (expectedPartitions <= 0) return;
  while (true) {
    const md = await admin.fetchTopicMetadata({ topics: [topic] });
    const t = md.topics.find((x) => x.name === topic);
    const leadersOk = !!t && t.partitions.length >= expectedPartitions && t.partitions.every((p) => p.leader !== -1);
    if (leadersOk) return;
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
async function safeDescribeConfigs(admin, topics2) {
  try {
    const resp = await admin.describeConfigs?.(
      {
        resources: topics2.map((name) => ({
          type: CONFIG_RESOURCE_TYPE_TOPIC,
          name
        })),
        includeSynonyms: false,
        includeDocumentation: false
      }
    );
    return resp;
  } catch {
    return void 0;
  }
}
async function ensureTopicsWithConfig(admin, opts = {}, manageConnection = true) {
  const partitionsDefault = opts.partitions ?? 3;
  const rfDefault = opts.replicationFactor ?? 1;
  const waitForLeadersMs = opts.waitForLeadersMs ?? 15e3;
  const flatTopics = Object.fromEntries(
    Object.values(topics).flatMap(
      (group) => Object.values(group).map((t) => [
        t.name,
        { name: t.name, config: t }
      ])
    )
  );
  const defs = asTopicDefs(flatTopics);
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
      logger.info("creating Kafka topics", {
        topics: createList.map((t) => t.topic)
      });
      try {
        await admin.createTopics({ topics: createList, waitForLeaders: true });
      } catch (e) {
        if (!/already exists/i.test(e?.message || "")) throw e;
      }
      for (const t of createList) {
        await waitForLeaders(
          admin,
          t.topic,
          t.numPartitions ?? partitionsDefault,
          waitForLeadersMs
        );
      }
    }
    const md2 = await admin.fetchTopicMetadata();
    const current = new Map(md2.topics.map((t) => [t.name, t]));
    const partitionsToIncrease = defs.map((t) => {
      const desired = t.config.partitions ?? partitionsDefault;
      const curCount = current.get(t.name)?.partitions?.length ?? 0;
      return desired > curCount ? { topic: t.name, count: desired } : null;
    }).filter(Boolean);
    if (partitionsToIncrease.length) {
      logger.info("increasing partitions", partitionsToIncrease);
      try {
        await admin.createPartitions({ topicPartitions: partitionsToIncrease });
      } catch (e) {
        logger.warn("createPartitions returned error; continuing", {
          error: e?.message
        });
      }
      for (const t of partitionsToIncrease) {
        await waitForLeaders(admin, t.topic, t.count, waitForLeadersMs);
      }
    }
    const topics2 = defs.map((t) => t.name);
    const desiredByTopic = new Map(
      defs.map((t) => [t.name, toConfigEntries(t.config)])
    );
    const desc = await safeDescribeConfigs(admin, topics2);
    let resources;
    if (desc?.resources?.length) {
      const currentByTopic = new Map(
        desc.resources.map((r) => [
          r.resourceName,
          new Map((r.configs || []).map((c) => [c.name, c.value]))
        ])
      );
      const pending = [];
      for (const name of topics2) {
        const desired = desiredByTopic.get(name);
        const currentCfg = currentByTopic.get(name);
        const diff = desired.filter(
          (entry) => currentCfg?.get(entry.name) !== entry.value
        );
        if (diff.length)
          pending.push({
            type: CONFIG_RESOURCE_TYPE_TOPIC,
            name,
            configEntries: diff
          });
      }
      resources = pending;
    } else {
      resources = topics2.map((name) => ({
        type: CONFIG_RESOURCE_TYPE_TOPIC,
        name,
        configEntries: desiredByTopic.get(name)
      }));
    }
    if (resources.length) {
      try {
        await admin.alterConfigs({ resources, validateOnly: false });
        logger.info("topic configs ensured", {
          topics: resources.map((r) => r.name)
        });
      } catch (e) {
        logger.warn(
          "alterConfigs failed (check ACLs / broker version). Topics still created.",
          {
            error: e?.message
          }
        );
      }
    } else {
      logger.debug?.("topic configs already up-to-date");
    }
  } finally {
    if (manageConnection) await admin.disconnect();
  }
}

export { ensureTopicsWithConfig };
