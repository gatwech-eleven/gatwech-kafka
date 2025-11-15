import { z } from "zod";
import { BaseProducerSchema } from "./base.schema";

export const OrderCreatedSchema = BaseProducerSchema.extend({
  id: z.string(),
  userId: z.string(),
  amount: z.number(),
  items: z.array(
    z.object({
      productId: z.string(),
      quantity: z.number(),
      price: z.number(),
    })
  ),
});

export const OrderUpdatedSchema = BaseProducerSchema.extend({
  id: z.string(),
  status: z.string(),
});

export const OrderCancelledSchema = BaseProducerSchema.extend({
  id: z.string(),
  reason: z.string().optional(),
});

export type TOrderCreated = z.infer<typeof OrderCreatedSchema>;
export type TOrderUpdated = z.infer<typeof OrderUpdatedSchema>;
export type TOrderCancelled = z.infer<typeof OrderCancelledSchema>;
