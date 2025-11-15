import { ChatMessageSentSchema } from "../../schemas";
import { TopicConfigSchema } from "./topic-config-schema";

export const chatTopics = {
  CHAT_MESSAGE_SENT: TopicConfigSchema.parse({
    name: "chat.message.sent",
    schema: ChatMessageSentSchema,
  }),
};
