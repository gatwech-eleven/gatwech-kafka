import {
  OrderCancelledSchema,
  OrderCreatedSchema,
  OrderUpdatedSchema,
} from "../../schemas";
import { TopicConfigSchema } from "./topic-config-schema";

export const orderTopics = {
  ORDER_CREATED: TopicConfigSchema.parse({
    name: "order.created",
    schema: OrderCreatedSchema,
  }),
  ORDER_UPDATED: TopicConfigSchema.parse({
    name: "order.updated",
    schema: OrderUpdatedSchema,
  }),
  ORDER_CANCELLED: TopicConfigSchema.parse({
    name: "order.cancelled",
    schema: OrderCancelledSchema,
  }),
};
