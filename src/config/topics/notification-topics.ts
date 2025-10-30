import { TopicConfigSchema } from "./topic-config-schema";

 
export const notificationTopics = {
  EMAIL_SENT: TopicConfigSchema.parse({ name: "notification.email.sent" }),
  SMS_SENT: TopicConfigSchema.parse({ name: "notification.sms.sent" }),
};
