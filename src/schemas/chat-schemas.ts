import { z } from "zod";
import { BaseProducerSchema } from "./base.schema";

export const ChatMessageSentSchema = BaseProducerSchema.extend({
  chatId: z.string(),
  userId: z.string(),
  message: z.string(),
});

export type TChatMessageSent = z.infer<typeof ChatMessageSentSchema>;
