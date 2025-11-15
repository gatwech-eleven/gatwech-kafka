import {
  PaymentCompletedSchema,
  PaymentFailedSchema,
  PaymentInitiatedSchema,
} from "../../schemas";
import { TopicConfigSchema } from "./topic-config-schema";

export const paymentTopics = {
  PAYMENT_INITIATED: TopicConfigSchema.parse({
    name: "payment.initiated",
    schema: PaymentInitiatedSchema,
  }),
  PAYMENT_COMPLETED: TopicConfigSchema.parse({
    name: "payment.completed",
    schema: PaymentCompletedSchema,
  }),
  PAYMENT_FAILED: TopicConfigSchema.parse({
    name: "payment.failed",
    schema: PaymentFailedSchema,
  }),
};
