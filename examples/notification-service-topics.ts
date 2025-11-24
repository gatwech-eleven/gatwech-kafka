/**
 * Example: Application-Level Topic Configuration for a Notification Service
 *
 * This example demonstrates how to define Kafka topics at the application level
 * using the new `createTopicConfig` API with strict type safety and Zod validation.
 */

import { z } from "@gatwech/utils/zod";
import {
  createTopicConfig,
  TopicConfigBuilder,
  topicBuilder,
} from "../src";
import { createKafkaClient } from "../src/client/core/create-client";
import { topicProvisioningExtension } from "../src/extensions/topic-provisioning";

// Define your message schemas
const UserRegisteredSchema = z.object({
  id: z.string(),
  email: z.string().email(),
  name: z.string(),
  timestamp: z.number(),
});

const NotificationSentSchema = z.object({
  id: z.string(),
  userId: z.string(),
  type: z.enum(["email", "sms", "push"]),
  message: z.string(),
  sentAt: z.number(),
});

const NotificationFailedSchema = z.object({
  id: z.string(),
  userId: z.string(),
  error: z.string(),
  failedAt: z.number(),
});

// Method 1: Using createTopicConfig (Recommended)

export const notificationTopics = createTopicConfig({
  // User events that trigger notifications
  USER_REGISTERED: {
    name: "user.registered",
    schema: UserRegisteredSchema,
    retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
    cleanupPolicy: "delete",
    partitions: 3,
  },

  // Notification events
  NOTIFICATION_SENT: {
    name: "notification.sent",
    schema: NotificationSentSchema,
    retentionMs: 30 * 24 * 60 * 60 * 1000, // 30 days
    cleanupPolicy: "compact,delete", // Keep latest + time-based deletion
    partitions: 6,
  },

  NOTIFICATION_FAILED: {
    name: "notification.failed",
    schema: NotificationFailedSchema,
    retentionMs: 90 * 24 * 60 * 60 * 1000, // 90 days (for debugging)
    cleanupPolicy: "delete",
  },
});

// Type-safe access to topic names
const userTopicName = notificationTopics.USER_REGISTERED.name; // TopicName (branded)
const userSchema = notificationTopics.USER_REGISTERED.schema; // ZodType

// Method 2: Using TopicConfigBuilder (Fluent API)

export const analyticsTopics = {
  PAGE_VIEW: new TopicConfigBuilder("analytics.page_view")
    .withSchema(z.unknown()) // Schema is now required
    .withRetentionDays(30)
    .withCleanupPolicy("delete")
    .withPartitions(12)
    .build(),

  USER_ACTION: topicBuilder("analytics.user_action")
    .withSchema(z.unknown()) // Schema is now required
    .withRetentionHours(24 * 90) // 90 days
    .withCleanupPolicy("compact,delete")
    .withPartitions(6)
    .withReplicationFactor(2)
    .build(),
};

// Initialize Topics in Your Application

async function initializeNotificationServiceTopics() {
  const client = createKafkaClient({
    config: {
      clientId: "notification-service",
      brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
      metricsEnabled: false,
    },
    extensions: [
      topicProvisioningExtension(notificationTopics, {
        partitions: 3, // Default for topics without explicit partitions
        replicationFactor: 1, // Default for local dev
      }),
    ],
  });

  await client.connect();

  try {
    // Initialize application-specific topics only
    await client.extensions.topicProvisioning.provisionTopics();

    console.log("✅ Notification service topics initialized");
  } finally {
    await client.disconnect();
  }
}

// Mixed: Application + Built-in Topics

async function initializeWithBuiltInTopics() {
  const client = createKafkaClient({
    config: {
      clientId: "my-service",
      brokers: ["localhost:9092"],
    },
    extensions: [
      topicProvisioningExtension(notificationTopics, {
        partitions: 3,
        replicationFactor: 1,
      }),
    ],
  });

  await client.connect();

  try {
    // Use both application topics AND built-in package topics
    await client.extensions.topicProvisioning.provisionTopics();

    console.log("✅ All topics initialized (app + built-in)");
  } finally {
    await client.disconnect();
  }
}

// Type Safety Benefits

// ✅ Compile-time type checking
const topic = notificationTopics.USER_REGISTERED;
// topic.name is branded as TopicName
// topic.schema is the Zod schema
// topic.retentionMs is number
// topic.cleanupPolicy is "delete" | "compact" | "compact,delete"

// ✅ Runtime validation
try {
  const invalidTopic = createTopicConfig({
    INVALID: {
      name: "", // ❌ Will throw: "Topic name cannot be empty"
      retentionMs: -1000, // ❌ Will throw: "Retention must be positive"
      schema: z.unknown(), // Schema is now required
    },
  });
} catch (error) {
  console.error("Validation failed:", error);
}

// Export for use in your application

export { initializeNotificationServiceTopics, initializeWithBuiltInTopics };
