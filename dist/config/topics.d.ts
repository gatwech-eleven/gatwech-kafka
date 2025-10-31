import { z } from '@payvo/utils/zod';
import { a as TopicConfigSchema } from '../shared/kafka.TtePgHi5.js';

declare const chatTopics: {
    CHAT_MESSAGE_SENT: any;
};

declare const userTopics: {
    USER_REGISTERED: any;
    USER_UPDATED: any;
    USER_LOGGED_IN: any;
    USER_LOGGED_OUT: {
        name: string;
        config: {
            retentionMs: number;
            cleanupPolicy: string;
        };
    };
    USER_PASSWORD_RESET: any;
    USER_RESET_PASSWORD_REQUEST: any;
    USER_VERIFIED: any;
    USER_KYC_APPLICATION_CREATED: any;
    USER_KYC_APPLICATION_UPDATED: any;
    USER_KYC_DOCUMENTS_UPLOADED: any;
    USER_KYC_DOCUMENTS_REVIEWED: any;
    USER_KYC_DOCUMENTS_APPROVED: any;
    USER_KYC_DOCUMENTS_REJECTED: any;
    USER_KYC_APPLICATION_COMPLETED: any;
    USER_PHONE_NUMBER_ADDED: any;
    USER_PHONE_NUMBER_REMOVED: any;
    USER_PHONE_NUMBER_VERIFIED: any;
    USER_SUSPENDED: any;
    USER_UNSUSPENDED: any;
};

declare const paymentTopics: {
    PAYMENT_CREATED: any;
    PAYMENT_COMPLETED: any;
    PAYMENT_FAILED: any;
};

declare const orderTopics: {
    ORDER_CREATED: any;
    ORDER_UPDATED: any;
    ORDER_CANCELED: any;
    ORDER_COMPLETED: any;
    ORDER_DISPUTED: any;
};

declare const notificationTopics: {
    EMAIL_SENT: any;
    SMS_SENT: any;
};

type TopicConfig = z.infer<typeof TopicConfigSchema>;

declare const topics: {
    user: {
        USER_REGISTERED: any;
        USER_UPDATED: any;
        USER_LOGGED_IN: any;
        USER_LOGGED_OUT: {
            name: string;
            config: {
                retentionMs: number;
                cleanupPolicy: string;
            };
        };
        USER_PASSWORD_RESET: any;
        USER_RESET_PASSWORD_REQUEST: any;
        USER_VERIFIED: any;
        USER_KYC_APPLICATION_CREATED: any;
        USER_KYC_APPLICATION_UPDATED: any;
        USER_KYC_DOCUMENTS_UPLOADED: any;
        USER_KYC_DOCUMENTS_REVIEWED: any;
        USER_KYC_DOCUMENTS_APPROVED: any;
        USER_KYC_DOCUMENTS_REJECTED: any;
        USER_KYC_APPLICATION_COMPLETED: any;
        USER_PHONE_NUMBER_ADDED: any;
        USER_PHONE_NUMBER_REMOVED: any;
        USER_PHONE_NUMBER_VERIFIED: any;
        USER_SUSPENDED: any;
        USER_UNSUSPENDED: any;
    };
    payment: {
        PAYMENT_CREATED: any;
        PAYMENT_COMPLETED: any;
        PAYMENT_FAILED: any;
    };
    notification: {
        EMAIL_SENT: any;
        SMS_SENT: any;
    };
};
type TopicGroups = typeof topics;
type TopicNames = keyof TopicGroups[keyof TopicGroups];

export { chatTopics, notificationTopics, orderTopics, paymentTopics, topics, userTopics };
export type { TopicConfig, TopicGroups, TopicNames };
