import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

// Test placeholder schema
export const ResetPasswordRequestEmailSchema = BaseProducerSchema.extend({
  userId: z.string().uuid().optional(),
  identifier: z.literal("email"),
  email: z.string().email(),
  token: z.string(),
});
