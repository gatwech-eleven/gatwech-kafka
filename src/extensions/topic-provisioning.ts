import type { KafkaClient } from "../client/kafka";
import type { KafkaExtension } from "../client/create-client";
import { initializeKafkaTopics } from "../config/ensure-topics";
import type { TopicConfig, ValidatedTopicConfig } from "../config/topics/topic-config-schema";

export interface TopicProvisioningOptions {
  /**
   * Default number of partitions for new topics
   */
  partitions?: number;
  /**
   * Default replication factor for new topics
   */
  replicationFactor?: number;
  /**
   * Max time to wait for leaders after create/partition increase
   */
  waitForLeadersMs?: number;
  /**
   * Whether to include built-in package topics
   */
  includeBuiltIn?: boolean;
}

/**
 * Extension that adds topic provisioning capabilities to the Kafka client.
 *
 * @param topics - Map of topic configurations (from createTopicConfig)
 * @param options - Provisioning options
 */
export function topicProvisioningExtension(
  topics?: Record<string, ValidatedTopicConfig | TopicConfig>,
  options: TopicProvisioningOptions = {}
): KafkaExtension<{ provisionTopics: () => Promise<void> }> {
  return {
    name: "topicProvisioning",
    setup: (client: KafkaClient) => {
      return {
        /**
         * Provision the configured topics.
         * Creates missing topics, updates configs, and increases partitions if needed.
         */
        provisionTopics: async () => {
          const admin = client.getAdmin();
          await initializeKafkaTopics(admin, {
            topics,
            partitions: options.partitions,
            replicationFactor: options.replicationFactor,
            waitForLeadersMs: options.waitForLeadersMs,
            includeBuiltIn: options.includeBuiltIn,
          });
        },
      };
    },
  };
}
