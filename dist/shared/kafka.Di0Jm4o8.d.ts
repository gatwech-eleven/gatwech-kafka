import { z } from '@payvo/utils/zod';

declare const UserRegisteredSchema: any;
declare const ResetPasswordRequestEmailSchema: any;
declare const ResetPasswordRequestPhoneSchema: any;
declare const ResetPasswordRequestUnionSchema: any;
type TUserRegistered = z.infer<typeof UserRegisteredSchema>;
type TResetPasswordRequest = z.infer<typeof ResetPasswordRequestUnionSchema>;
declare const _default$1: {
    UserRegisteredSchema: any;
    ResetPasswordRequestEmailSchema: any;
    ResetPasswordRequestPhoneSchema: any;
    ResetPasswordRequestUnionSchema: any;
};

declare const NotificationEmailSentSchema: any;
type TNotificationEmailSent = z.infer<typeof NotificationEmailSentSchema>;
declare const _default: {
    NotificationEmailSentSchema: any;
};

declare const index_NotificationEmailSentSchema: typeof NotificationEmailSentSchema;
declare const index_ResetPasswordRequestEmailSchema: typeof ResetPasswordRequestEmailSchema;
declare const index_ResetPasswordRequestPhoneSchema: typeof ResetPasswordRequestPhoneSchema;
declare const index_ResetPasswordRequestUnionSchema: typeof ResetPasswordRequestUnionSchema;
type index_TNotificationEmailSent = TNotificationEmailSent;
type index_TResetPasswordRequest = TResetPasswordRequest;
type index_TUserRegistered = TUserRegistered;
declare const index_UserRegisteredSchema: typeof UserRegisteredSchema;
declare namespace index {
  export { index_NotificationEmailSentSchema as NotificationEmailSentSchema, index_ResetPasswordRequestEmailSchema as ResetPasswordRequestEmailSchema, index_ResetPasswordRequestPhoneSchema as ResetPasswordRequestPhoneSchema, index_ResetPasswordRequestUnionSchema as ResetPasswordRequestUnionSchema, index_UserRegisteredSchema as UserRegisteredSchema, _default as notificationSchemas, _default$1 as userSchemas };
  export type { index_TNotificationEmailSent as TNotificationEmailSent, index_TResetPasswordRequest as TResetPasswordRequest, index_TUserRegistered as TUserRegistered };
}

export { NotificationEmailSentSchema as N, ResetPasswordRequestEmailSchema as R, UserRegisteredSchema as U, _default$1 as _, _default as a, ResetPasswordRequestPhoneSchema as b, ResetPasswordRequestUnionSchema as c, index as i };
export type { TUserRegistered as T, TResetPasswordRequest as d, TNotificationEmailSent as e };
