// src/utils/logger.ts
import { createLogger, format, transports } from "winston";
var { combine, timestamp, printf, colorize } = format;
var defaultFormat = printf(
  ({ level, message, timestamp: timestamp2, service, ...meta }) => {
    const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : "";
    return `${timestamp2} [${service || "kafka"}] ${level}: ${message}${metaStr}`;
  }
);
function createServiceLogger(serviceName = "kafka") {
  return createLogger({
    level: process.env.LOG_LEVEL || (process.env.NODE_ENV === "production" ? "info" : "debug"),
    format: combine(
      timestamp(),
      colorize({ all: true }),
      format((info) => ({ ...info, service: serviceName }))(),
      defaultFormat
    ),
    transports: [new transports.Console()]
  });
}
var loggerInstance = createServiceLogger(process.env.SERVICE_NAME || "kafka");
var logger = loggerInstance;

// src/config/topics/topic-config-schema.ts
import { z } from "@payvo/utils/zod";
var TopicConfigSchema = z.object({
  name: z.string(),
  retentionMs: z.number().default(6048e5),
  cleanupPolicy: z.enum(["delete", "compact", "compact,delete"]).default("delete")
});

// src/config/topics/user-topics.ts
var userTopics = {
  // User Auth Topics
  USER_REGISTERED: TopicConfigSchema.parse({
    name: "user.registered"
  }),
  USER_UPDATED: TopicConfigSchema.parse({
    name: "user.updated"
  }),
  USER_LOGGED_IN: TopicConfigSchema.parse({
    name: "user.logged.in"
  }),
  USER_LOGGED_OUT: {
    name: "user.logged.out",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  },
  USER_PASSWORD_RESET: TopicConfigSchema.parse({
    name: "user.password.reset"
  }),
  USER_RESET_PASSWORD_REQUEST: TopicConfigSchema.parse({
    name: "user.reset.password.request",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  }),
  USER_VERIFIED: TopicConfigSchema.parse({
    name: "user.verified",
    config: { retentionMs: 6048e5, cleanupPolicy: "compact" }
  }),
  // User KYC Topics
  USER_KYC_APPLICATION_CREATED: TopicConfigSchema.parse({
    name: "user.kyc.application.created"
  }),
  USER_KYC_APPLICATION_UPDATED: TopicConfigSchema.parse({
    name: "user.kyc.application.updated"
  }),
  USER_KYC_DOCUMENTS_UPLOADED: TopicConfigSchema.parse({
    name: "user.kyc.documents.uploaded"
  }),
  USER_KYC_DOCUMENTS_REVIEWED: TopicConfigSchema.parse({
    name: "user.kyc.documents.reviewed"
  }),
  USER_KYC_DOCUMENTS_APPROVED: TopicConfigSchema.parse({
    name: "user.kyc.documents.approved"
  }),
  USER_KYC_DOCUMENTS_REJECTED: TopicConfigSchema.parse({
    name: "user.kyc.documents.rejected"
  }),
  USER_KYC_APPLICATION_COMPLETED: TopicConfigSchema.parse({
    name: "user.kyc.application.completed"
  }),
  // Phone Number Topics
  USER_PHONE_NUMBER_ADDED: TopicConfigSchema.parse({
    name: "user.phone.number.added"
  }),
  USER_PHONE_NUMBER_REMOVED: TopicConfigSchema.parse({
    name: "user.phone.number.removed"
  }),
  USER_PHONE_NUMBER_VERIFIED: TopicConfigSchema.parse({
    name: "user.phone.number.verified"
  }),
  // User Suspension Topics
  USER_SUSPENDED: TopicConfigSchema.parse({ name: "user.suspended" }),
  USER_UNSUSPENDED: TopicConfigSchema.parse({ name: "user.unsuspended" })
};

// src/config/topics/payment-topics.ts
var paymentTopics = {
  PAYMENT_CREATED: TopicConfigSchema.parse({ name: "payment.created" }),
  PAYMENT_COMPLETED: TopicConfigSchema.parse({ name: "payment.completed" }),
  PAYMENT_FAILED: TopicConfigSchema.parse({ name: "payment.failed" })
};

// src/config/topics/notification-topics.ts
var notificationTopics = {
  EMAIL_SENT: TopicConfigSchema.parse({ name: "notification.email.sent" }),
  SMS_SENT: TopicConfigSchema.parse({ name: "notification.sms.sent" })
};

// src/config/topics/chat-topic.ts
var chatTopics = {
  CHAT_MESSAGE_SENT: TopicConfigSchema.parse({
    name: "notification.chat.message.sent"
  })
};

// src/config/topics/order-topics.ts
var orderTopics = {
  ORDER_CREATED: TopicConfigSchema.parse({ name: "order.created" }),
  ORDER_UPDATED: TopicConfigSchema.parse({ name: "order.updated" }),
  ORDER_CANCELED: TopicConfigSchema.parse({ name: "order.canceled" }),
  ORDER_COMPLETED: TopicConfigSchema.parse({ name: "order.completed" }),
  ORDER_DISPUTED: TopicConfigSchema.parse({ name: "order.disputed" })
};

// src/config/topics/index.ts
var topics = {
  user: userTopics,
  payment: paymentTopics,
  notification: notificationTopics
};

// src/config/ensure-topics.ts
var CONFIG_RESOURCE_TYPE_TOPIC = 2;
var asTopicDefs = (topics2) => Object.values(topics2).map((t) => ({
  name: t.name,
  config: t.config
}));
var toConfigEntries = (cfg) => [
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
export {
  ensureTopicsWithConfig
};
