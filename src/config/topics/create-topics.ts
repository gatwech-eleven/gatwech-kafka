import type { Admin } from "kafkajs";
import { logger } from "../../utils/logger";
import {
  toConfigEntries,
  waitForLeaders,
  withRetry,
} from "./utils";
import type { TopicDef } from "./utils";

/**
 * Create topics that don't exist yet.
 * 
 * @param admin - KafkaJS Admin client
 * @param defs - Normalized topic definitions
 * @param existing - Set of existing topic names
 * @param partitionsDefault - Default number of partitions
 * @param rfDefault - Default replication factor
 * @param waitForLeadersMs - Max time to wait for leaders
 */
export async function createMissingTopics(
  admin: Admin,
  defs: TopicDef[],
  existing: Set<string>,
  partitionsDefault: number,
  rfDefault: number,
  waitForLeadersMs: number
): Promise<void> {
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
    } catch (e: unknown) {
      // Race between services; ignore benign "already exists"
      const errorMessage = e instanceof Error ? e.message : String(e);
      if (!/already exists/i.test(errorMessage)) throw e;
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
}
