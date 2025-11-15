import { z } from "@gatwech/utils/zod";

export const BaseProducerSchema = z.object({
  timestamp: z
    .date()
    .optional()
    .default(() => new Date()),
  metadata: z.any().optional(),
});
