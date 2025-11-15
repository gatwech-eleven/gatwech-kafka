import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

export const DlqMessageSchema = BaseProducerSchema.extend({
  originalTopic: z.string(),
  originalPartition: z.number().int().nonnegative(),
  originalOffset: z.string(),
  consumerGroup: z.string(),
  errorMessage: z.string(),
  stackTrace: z.string().optional(),
  originalKey: z.string().optional(),
  originalHeaders: z.record(z.string(), z.string()).optional(),
  // Keep the original payload as-is; allow any to avoid losing information
  originalValue: z.any(),
});

export type TDlqMessage = z.infer<typeof DlqMessageSchema>;

export default {
  DlqMessageSchema,
};
