import type { Admin } from "kafkajs";
import { logger } from "../../utils/logger";
import {
  CONFIG_RESOURCE_TYPE_TOPIC,
  safeDescribeConfigs,
  toConfigEntries,
} from "./utils";
import type { TopicDef } from "./utils";

/**
 * Update topic configurations (retention, cleanup policy, etc).
 * 
 * @param admin - KafkaJS Admin client
 * @param defs - Normalized topic definitions
 */
export async function updateTopicConfigs(
  admin: Admin,
  defs: TopicDef[]
): Promise<void> {
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
    } catch (e: unknown) {
      const errorMessage = e instanceof Error ? e.message : String(e);
      logger.warn(
        "alterConfigs failed (check ACLs / broker version). Topics still created.",
        {
          error: errorMessage,
        }
      );
    }
  } else {
    logger.debug("topic configs already up-to-date");
  }
}
