import { chatTopics } from "./chat-topic";
import { orderTopics } from "./order-topics";
import { paymentTopics } from "./payment-topics";
import { userTopics } from "./user-topics";
import { notificationTopics } from "./notification-topics";

export const topics = {
  ...userTopics,
  ...notificationTopics,
  ...orderTopics,
  ...paymentTopics,
  ...chatTopics,
};

export * from "./user-topics";
export * from "./notification-topics";
export * from "./order-topics";
export * from "./payment-topics";
export * from "./chat-topic";
export * from "./topic-config-schema";
