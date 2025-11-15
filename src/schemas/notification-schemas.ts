import { z } from "@gatwech/utils/zod";

export const NotificationEmailSentSchema = z.object({
  to: z.string().email(),
  subject: z.string(),
  body: z.string(),
  timestamp: z.date().default(() => new Date()),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});

export type TNotificationEmailSent = z.infer<
  typeof NotificationEmailSentSchema
>;

export default { NotificationEmailSentSchema };
