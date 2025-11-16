import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

export const UserRegisteredSchema = BaseProducerSchema.extend({
  id: z.string(),
  email: z.email(),
  name: z.string().optional(),
  otp: z.string().optional(),
});

export const ResetPasswordRequestEmailSchema = BaseProducerSchema.extend({
  userId: z.uuid().optional(),
  identifier: z.literal("email"),
  email: z.email(),
  token: z.string(),
});

export const ResetPasswordRequestPhoneSchema = BaseProducerSchema.extend({
  userId: z.uuid().optional(),
  identifier: z.literal("phone"),
  phoneNumber: z.string(),
  otp: z.string().optional(),
  token: z.string().optional(),
});

export const ResetPasswordRequestUnionSchema = z.union([
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema,
]);

export type TUserRegistered = z.infer<typeof UserRegisteredSchema>;
export type TResetPasswordRequest = z.infer<
  typeof ResetPasswordRequestUnionSchema
>;

export default {
  UserRegisteredSchema,
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema,
  ResetPasswordRequestUnionSchema,
};
