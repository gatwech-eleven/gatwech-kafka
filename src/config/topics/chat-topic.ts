import { TopicConfigSchema } from "./topic-config-schema";

export const chatTopics = {
  CHAT_MESSAGE_SENT: TopicConfigSchema.parse({
    name: "notification.chat.message.sent",
  }),
};
