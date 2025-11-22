// Built-in topics (optional defaults)
import { chatTopics } from "./chat-topic";
import { orderTopics } from "./order-topics";
import { paymentTopics } from "./payment-topics";
import { userTopics } from "./user-topics";
import { notificationTopics } from "./notification-topics";

/**
 * Built-in Kafka topics provided by the package.
 * These are optional defaults that can be used with `includeBuiltIn: true`.
 *
 * For application-level topic definitions, use `createTopicConfig()` instead.
 */
export const topics = {
  ...userTopics,
  ...notificationTopics,
  ...orderTopics,
  ...paymentTopics,
  ...chatTopics,
};

// Export individual topic collections
export * from "./user-topics";
export * from "./notification-topics";
export * from "./order-topics";
export * from "./payment-topics";
export * from "./chat-topic";

// Export topic configuration schema and types
export * from "./topic-config-schema";

// Export new application-level topic definition API
export * from "./define-topics";
export * from "./topic-builder";

