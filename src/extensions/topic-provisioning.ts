import type { KafkaClient } from "../client/core/kafka";
import type { KafkaExtension } from "../client/core/create-client";
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
): KafkaExtension<{ provisionTopics: () => Promise<void> }, "topicProvisioning"> {
  return {
    name: "topicProvisioning",
    setup: (client: KafkaClient) => {
      return {
        /**
         * Provision the configured topics.
         * Creates missing topics, updates configs, and increases partitions if needed.
         */
        provisionTopics: async () => {
          // Use provided topics or fall back to client.topics
          const topicsToProvision = topics || client.topics;
          
          if (!topicsToProvision) {
            throw new Error(
              'No topics configured. Either pass topics to topicProvisioningExtension() or configure them in client options.'
            );
          }

          const admin = client.getAdmin();
          await initializeKafkaTopics(admin, {
            topics: topicsToProvision,
            partitions: options.partitions,
            replicationFactor: options.replicationFactor,
            waitForLeadersMs: options.waitForLeadersMs,
          });
        },
      };
    },
  };
}
