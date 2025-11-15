import { n as TopicConfigSchema } from "./topic-config-schema-Byptn_Fn.cjs";
import { z } from "@gatwech/utils/zod";

//#region src/config/topics/chat-topic.d.ts
declare const chatTopics: {
  CHAT_MESSAGE_SENT: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
};
//#endregion
//#region src/config/topics/user-topics.d.ts
declare const userTopics: {
  USER_REGISTERED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_UPDATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_LOGGED_IN: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_LOGGED_OUT: {
    name: string;
    config: {
      retentionMs: number;
      cleanupPolicy: string;
    };
  };
  USER_PASSWORD_RESET: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_RESET_PASSWORD_REQUEST: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_VERIFIED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_APPLICATION_CREATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_APPLICATION_UPDATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_DOCUMENTS_UPLOADED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_DOCUMENTS_REVIEWED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_DOCUMENTS_APPROVED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_DOCUMENTS_REJECTED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_KYC_APPLICATION_COMPLETED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_PHONE_NUMBER_ADDED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_PHONE_NUMBER_REMOVED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_PHONE_NUMBER_VERIFIED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_SUSPENDED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  USER_UNSUSPENDED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
};
//#endregion
//#region src/config/topics/payment-topics.d.ts
declare const paymentTopics: {
  PAYMENT_CREATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  PAYMENT_COMPLETED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  PAYMENT_FAILED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
};
//#endregion
//#region src/config/topics/order-topics.d.ts
declare const orderTopics: {
  ORDER_CREATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  ORDER_UPDATED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  ORDER_CANCELED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  ORDER_COMPLETED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  ORDER_DISPUTED: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
};
//#endregion
//#region src/config/topics/notification-topics.d.ts
declare const notificationTopics: {
  EMAIL_SENT: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
  SMS_SENT: {
    name: string;
    retentionMs: number;
    cleanupPolicy: "delete" | "compact" | "compact,delete";
  };
};
//#endregion
//#region src/config/topics/index.d.ts
type TopicConfig = z.infer<typeof TopicConfigSchema>;
declare const topics: {
  user: {
    USER_REGISTERED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_UPDATED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_LOGGED_IN: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_LOGGED_OUT: {
      name: string;
      config: {
        retentionMs: number;
        cleanupPolicy: string;
      };
    };
    USER_PASSWORD_RESET: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_RESET_PASSWORD_REQUEST: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_VERIFIED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_APPLICATION_CREATED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_APPLICATION_UPDATED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_DOCUMENTS_UPLOADED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_DOCUMENTS_REVIEWED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_DOCUMENTS_APPROVED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_DOCUMENTS_REJECTED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_KYC_APPLICATION_COMPLETED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_PHONE_NUMBER_ADDED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_PHONE_NUMBER_REMOVED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_PHONE_NUMBER_VERIFIED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_SUSPENDED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    USER_UNSUSPENDED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
  };
  payment: {
    PAYMENT_CREATED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    PAYMENT_COMPLETED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    PAYMENT_FAILED: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
  };
  notification: {
    EMAIL_SENT: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
    SMS_SENT: {
      name: string;
      retentionMs: number;
      cleanupPolicy: "delete" | "compact" | "compact,delete";
    };
  };
};
type TopicGroups = typeof topics;
type TopicNames = keyof TopicGroups[keyof TopicGroups];
//#endregion
export { notificationTopics as a, userTopics as c, topics as i, chatTopics as l, TopicGroups as n, orderTopics as o, TopicNames as r, paymentTopics as s, TopicConfig as t };