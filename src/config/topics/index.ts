import type { z } from "@gatwech/utils/zod";
import { userTopics } from "./user-topics";
import { paymentTopics } from "./payment-topics";
import { notificationTopics } from "./notification-topics";
import type { TopicConfigSchema } from "./topic-config-schema";

export type TopicConfig = z.infer<typeof TopicConfigSchema>;

export * from "./chat-topic";
export * from "./user-topics";
export * from "./payment-topics";
export * from "./order-topics";
export * from "./notification-topics";

export const topics = {
  user: userTopics,
  payment: paymentTopics,
  notification: notificationTopics,
};

export type TopicGroups = typeof topics;
export type TopicNames = keyof TopicGroups[keyof TopicGroups];
