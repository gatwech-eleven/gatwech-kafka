import { TopicConfigSchema } from "./topic-config-schema";

export const orderTopics = {
  ORDER_CREATED: TopicConfigSchema.parse({ name: "order.created" }),
  ORDER_UPDATED: TopicConfigSchema.parse({ name: "order.updated" }),
  ORDER_CANCELED: TopicConfigSchema.parse({ name: "order.canceled" }),
  ORDER_COMPLETED: TopicConfigSchema.parse({ name: "order.completed" }),
  ORDER_DISPUTED: TopicConfigSchema.parse({ name: "order.disputed" }),
};
