import { z } from "@gatwech/utils/zod";

//#region src/config/topics/topic-config-schema.ts
const TopicConfigSchema = z.object({
	name: z.string(),
	retentionMs: z.number().default(6048e5),
	cleanupPolicy: z.enum([
		"delete",
		"compact",
		"compact,delete"
	]).default("delete")
});

//#endregion
//#region src/config/topics/user-topics.ts
const userTopics = {
	USER_REGISTERED: TopicConfigSchema.parse({ name: "user.registered" }),
	USER_UPDATED: TopicConfigSchema.parse({ name: "user.updated" }),
	USER_LOGGED_IN: TopicConfigSchema.parse({ name: "user.logged.in" }),
	USER_LOGGED_OUT: {
		name: "user.logged.out",
		config: {
			retentionMs: 6048e5,
			cleanupPolicy: "compact"
		}
	},
	USER_PASSWORD_RESET: TopicConfigSchema.parse({ name: "user.password.reset" }),
	USER_RESET_PASSWORD_REQUEST: TopicConfigSchema.parse({
		name: "user.reset.password.request",
		config: {
			retentionMs: 6048e5,
			cleanupPolicy: "compact"
		}
	}),
	USER_VERIFIED: TopicConfigSchema.parse({
		name: "user.verified",
		config: {
			retentionMs: 6048e5,
			cleanupPolicy: "compact"
		}
	}),
	USER_KYC_APPLICATION_CREATED: TopicConfigSchema.parse({ name: "user.kyc.application.created" }),
	USER_KYC_APPLICATION_UPDATED: TopicConfigSchema.parse({ name: "user.kyc.application.updated" }),
	USER_KYC_DOCUMENTS_UPLOADED: TopicConfigSchema.parse({ name: "user.kyc.documents.uploaded" }),
	USER_KYC_DOCUMENTS_REVIEWED: TopicConfigSchema.parse({ name: "user.kyc.documents.reviewed" }),
	USER_KYC_DOCUMENTS_APPROVED: TopicConfigSchema.parse({ name: "user.kyc.documents.approved" }),
	USER_KYC_DOCUMENTS_REJECTED: TopicConfigSchema.parse({ name: "user.kyc.documents.rejected" }),
	USER_KYC_APPLICATION_COMPLETED: TopicConfigSchema.parse({ name: "user.kyc.application.completed" }),
	USER_PHONE_NUMBER_ADDED: TopicConfigSchema.parse({ name: "user.phone.number.added" }),
	USER_PHONE_NUMBER_REMOVED: TopicConfigSchema.parse({ name: "user.phone.number.removed" }),
	USER_PHONE_NUMBER_VERIFIED: TopicConfigSchema.parse({ name: "user.phone.number.verified" }),
	USER_SUSPENDED: TopicConfigSchema.parse({ name: "user.suspended" }),
	USER_UNSUSPENDED: TopicConfigSchema.parse({ name: "user.unsuspended" })
};

//#endregion
//#region src/config/topics/payment-topics.ts
const paymentTopics = {
	PAYMENT_CREATED: TopicConfigSchema.parse({ name: "payment.created" }),
	PAYMENT_COMPLETED: TopicConfigSchema.parse({ name: "payment.completed" }),
	PAYMENT_FAILED: TopicConfigSchema.parse({ name: "payment.failed" })
};

//#endregion
//#region src/config/topics/notification-topics.ts
const notificationTopics = {
	EMAIL_SENT: TopicConfigSchema.parse({ name: "notification.email.sent" }),
	SMS_SENT: TopicConfigSchema.parse({ name: "notification.sms.sent" })
};

//#endregion
//#region src/config/topics/chat-topic.ts
const chatTopics = { CHAT_MESSAGE_SENT: TopicConfigSchema.parse({ name: "notification.chat.message.sent" }) };

//#endregion
//#region src/config/topics/order-topics.ts
const orderTopics = {
	ORDER_CREATED: TopicConfigSchema.parse({ name: "order.created" }),
	ORDER_UPDATED: TopicConfigSchema.parse({ name: "order.updated" }),
	ORDER_CANCELED: TopicConfigSchema.parse({ name: "order.canceled" }),
	ORDER_COMPLETED: TopicConfigSchema.parse({ name: "order.completed" }),
	ORDER_DISPUTED: TopicConfigSchema.parse({ name: "order.disputed" })
};

//#endregion
//#region src/config/topics/index.ts
const topics = {
	user: userTopics,
	payment: paymentTopics,
	notification: notificationTopics
};

//#endregion
export { paymentTopics as a, notificationTopics as i, orderTopics as n, userTopics as o, chatTopics as r, topics as t };