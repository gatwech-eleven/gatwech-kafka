import { TopicConfigSchema } from "./topic-config-schema";

 
export const paymentTopics = {
  PAYMENT_CREATED: TopicConfigSchema.parse({ name: "payment.created" }),
  PAYMENT_COMPLETED: TopicConfigSchema.parse({ name: "payment.completed" }),
  PAYMENT_FAILED: TopicConfigSchema.parse({ name: "payment.failed" }),
};
