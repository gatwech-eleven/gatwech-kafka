import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

export const NotificationEmailSentSchema = BaseProducerSchema.extend({
  to: z.email(),
  subject: z.string(),
  body: z.string(),
});

export type TNotificationEmailSent = z.infer<
  typeof NotificationEmailSentSchema
>;

export default { NotificationEmailSentSchema };
