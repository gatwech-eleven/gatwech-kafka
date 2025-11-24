import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

// User registration event schema
export const UserRegisteredSchema = BaseProducerSchema.extend({
  id: z.string(),
  email: z.string().email(),
  name: z.string(),
});

// Test placeholder schema
export const ResetPasswordRequestEmailSchema = BaseProducerSchema.extend({
  userId: z.uuid().optional(),
  identifier: z.literal("email"),
  email: z.string().email(),
  token: z.string(),
});
