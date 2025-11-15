import {
  ResetPasswordRequestUnionSchema,
  UserRegisteredSchema,
} from "../../schemas";
import { TopicConfigSchema } from "./topic-config-schema";

export const userTopics = {
  // User Auth Topics

  USER_REGISTERED: TopicConfigSchema.parse({
    name: "user.registered",
    schema: UserRegisteredSchema,
  }),
  USER_UPDATED: TopicConfigSchema.parse({
    name: "user.updated",
  }),
  USER_LOGGED_IN: TopicConfigSchema.parse({
    name: "user.logged.in",
  }),

  USER_LOGGED_OUT: {
    name: "user.logged.out",
    config: { retentionMs: 604800000, cleanupPolicy: "compact" },
  },
  USER_PASSWORD_RESET: TopicConfigSchema.parse({
    name: "user.password.reset",
  }),
  USER_RESET_PASSWORD_REQUEST: TopicConfigSchema.parse({
    name: "user.reset.password.request",
    config: { retentionMs: 604800000, cleanupPolicy: "compact" },
    schema: ResetPasswordRequestUnionSchema,
  }),
  USER_VERIFIED: TopicConfigSchema.parse({
    name: "user.verified",
    config: { retentionMs: 604800000, cleanupPolicy: "compact" },
  }),

  // User KYC Topics
  USER_KYC_APPLICATION_CREATED: TopicConfigSchema.parse({
    name: "user.kyc.application.created",
  }),
  USER_KYC_APPLICATION_UPDATED: TopicConfigSchema.parse({
    name: "user.kyc.application.updated",
  }),
  USER_KYC_DOCUMENTS_UPLOADED: TopicConfigSchema.parse({
    name: "user.kyc.documents.uploaded",
  }),
  USER_KYC_DOCUMENTS_REVIEWED: TopicConfigSchema.parse({
    name: "user.kyc.documents.reviewed",
  }),
  USER_KYC_DOCUMENTS_APPROVED: TopicConfigSchema.parse({
    name: "user.kyc.documents.approved",
  }),
  USER_KYC_DOCUMENTS_REJECTED: TopicConfigSchema.parse({
    name: "user.kyc.documents.rejected",
  }),
  USER_KYC_APPLICATION_COMPLETED: TopicConfigSchema.parse({
    name: "user.kyc.application.completed",
  }),

  // Phone Number Topics
  USER_PHONE_NUMBER_ADDED: TopicConfigSchema.parse({
    name: "user.phone.number.added",
  }),
  USER_PHONE_NUMBER_REMOVED: TopicConfigSchema.parse({
    name: "user.phone.number.removed",
  }),
  USER_PHONE_NUMBER_VERIFIED: TopicConfigSchema.parse({
    name: "user.phone.number.verified",
  }),

  // User Suspension Topics
  USER_SUSPENDED: TopicConfigSchema.parse({ name: "user.suspended" }),
  USER_UNSUSPENDED: TopicConfigSchema.parse({ name: "user.unsuspended" }),
};
