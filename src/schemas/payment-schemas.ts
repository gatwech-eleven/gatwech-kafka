import { z } from "zod";
import { BaseProducerSchema } from "./base.schema";

export const PaymentInitiatedSchema = BaseProducerSchema.extend({
  orderId: z.string(),
  amount: z.number(),
  method: z.string(),
});

export const PaymentCompletedSchema = BaseProducerSchema.extend({
  transactionId: z.string(),
  orderId: z.string(),
  amount: z.number(),
});

export const PaymentFailedSchema = BaseProducerSchema.extend({
  orderId: z.string(),
  reason: z.string(),
});

export type TPaymentInitiated = z.infer<typeof PaymentInitiatedSchema>;
export type TPaymentCompleted = z.infer<typeof PaymentCompletedSchema>;
export type TPaymentFailed = z.infer<typeof PaymentFailedSchema>;
