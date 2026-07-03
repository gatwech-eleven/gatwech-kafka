import type { Admin } from "kafkajs";
import { logger } from "../../utils/logger";
import {
  waitForLeaders,
  withRetry,
} from "./utils";
import type { TopicDef } from "./utils";

/**
 * Update partition counts for topics that need more partitions.
 * 
 * @param admin - KafkaJS Admin client
 * @param defs - Normalized topic definitions
 * @param current - Map of current topic metadata
 * @param partitionsDefault - Default number of partitions
 * @param waitForLeadersMs - Max time to wait for leaders
 */
export async function updatePartitionCounts(
  admin: Admin,
  defs: TopicDef[],
  current: Map<string, { name: string; partitions?: unknown[] }>,
  partitionsDefault: number,
  waitForLeadersMs: number
): Promise<void> {
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
    } catch (e: unknown) {
      // Some brokers may throw if already increased by another service; log and continue
      const errorMessage = e instanceof Error ? e.message : String(e);
      logger.warn("createPartitions returned error; continuing", {
        error: errorMessage,
      });
    }
    for (const t of partitionsToIncrease) {
      await waitForLeaders(admin, t.topic, t.count, waitForLeadersMs);
    }
  }
}
