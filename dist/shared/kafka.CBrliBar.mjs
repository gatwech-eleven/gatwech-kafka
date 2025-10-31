import { z } from '@payvo/utils/zod';

const UserRegisteredSchema = z.object({
  id: z.string(),
  email: z.email(),
  name: z.string().optional(),
  otp: z.string().optional(),
  timestamp: z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: z.object({
    correlationId: z.string().optional(),
    source: z.string().optional()
  }).optional()
});
const ResetPasswordRequestEmailSchema = z.object({
  userId: z.uuid().optional(),
  identifier: z.literal("email"),
  email: z.email(),
  token: z.string(),
  timestamp: z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: z.object({
    correlationId: z.string().optional(),
    source: z.string().optional()
  }).optional()
});
const ResetPasswordRequestPhoneSchema = z.object({
  userId: z.uuid().optional(),
  identifier: z.literal("phone"),
  phoneNumber: z.string(),
  otp: z.string().optional(),
  token: z.string().optional(),
  timestamp: z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: z.object({
    correlationId: z.string().optional(),
    source: z.string().optional()
  }).optional()
});
const ResetPasswordRequestUnionSchema = z.union([
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema
]);
const userSchemas = {
  UserRegisteredSchema,
  ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema,
  ResetPasswordRequestUnionSchema
};

const NotificationEmailSentSchema = z.object({
  to: z.string().email(),
  subject: z.string(),
  body: z.string(),
  timestamp: z.date().default(() => /* @__PURE__ */ new Date()),
  metadata: z.object({
    correlationId: z.string().optional(),
    source: z.string().optional()
  }).optional()
});
const notificationSchemas = { NotificationEmailSentSchema };

const index = {
  __proto__: null,
  NotificationEmailSentSchema: NotificationEmailSentSchema,
  ResetPasswordRequestEmailSchema: ResetPasswordRequestEmailSchema,
  ResetPasswordRequestPhoneSchema: ResetPasswordRequestPhoneSchema,
  ResetPasswordRequestUnionSchema: ResetPasswordRequestUnionSchema,
  UserRegisteredSchema: UserRegisteredSchema,
  notificationSchemas: notificationSchemas,
  userSchemas: userSchemas
};

export { NotificationEmailSentSchema as N, ResetPasswordRequestEmailSchema as R, UserRegisteredSchema as U, ResetPasswordRequestPhoneSchema as a, ResetPasswordRequestUnionSchema as b, index as i, notificationSchemas as n, userSchemas as u };
