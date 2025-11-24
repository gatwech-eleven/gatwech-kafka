import type { KafkaClient } from "../client/core/kafka";
import type { KafkaExtension } from "../client/core/create-client";

/**
 * Extension that provides health check functionality.
 *
 * While the base client has a healthCheck method, this extension allows
 * for more consistent access patterns in the modular architecture.
 */
export function healthCheckExtension(): KafkaExtension<{
  healthCheck: () => Promise<boolean>;
}, "healthCheck"> {
  return {
    name: "healthCheck",
    setup: (client: KafkaClient) => {
      // Bind the original method to avoid infinite recursion when Object.assign overwrites it
      const originalHealthCheck = client.healthCheck.bind(client);
      
      return {
        /**
         * Check if the Kafka client is healthy and connected to the broker.
         */
        healthCheck: async () => {
          return originalHealthCheck();
        },
      };
    },
  };
}
