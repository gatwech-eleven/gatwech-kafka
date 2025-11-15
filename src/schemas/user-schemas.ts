import { z } from "@gatwech/utils/zod";

export const UserRegisteredSchema = z.object({
  id: z.string(),
  email: z.email(),
  name: z.string().optional(),
  otp: z.string().optional(),
  timestamp: z.date().default(() => new Date()),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});

export const ResetPasswordRequestEmailSchema = z.object({
  userId: z.uuid().optional(),
  identifier: z.literal("email"),
  email: z.email(),
  token: z.string(),
  timestamp: z.date().default(() => new Date()),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});

export const ResetPasswordRequestPhoneSchema = z.object({
  userId: z.uuid().optional(),
  identifier: z.literal("phone"),
  phoneNumber: z.string(),
  otp: z.string().optional(),
  token: z.string().optional(),
  timestamp: z.date().default(() => new Date()),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
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
