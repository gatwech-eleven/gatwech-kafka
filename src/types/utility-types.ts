/** Make all properties in T deeply readonly */
export type DeepReadonly<T> = T extends Function
  ? T
  : T extends Array<infer U>
  ? ReadonlyArray<DeepReadonly<U>>
  : T extends object
  ? { readonly [K in keyof T]: DeepReadonly<T[K]> }
  : T;

/** Omit keys K from T but keep exact typing for the rest */
export type StrictOmit<T, K extends keyof any> = Pick<T, Exclude<keyof T, K>>;

/** Make some keys in T optional, others required */
export type OptionalExceptFor<T, K extends keyof T> = Partial<T> & Pick<T, K>;

/** Make a type with certain keys required and the rest optional */
export type RequireKeys<T, K extends keyof T> = T & { [P in K]-?: T[P] };

/** Mutable version of a readonly type */
export type Mutable<T> = { -readonly [P in keyof T]: T[P] };

/** Create a branded type */
export type Brand<T, B> = T & { __brand: B };

/** Branded correlation id string to prevent accidental mixing with other ids */
export type CorrelationId = Brand<string, "CorrelationId">;

/** Branded service source identifier (e.g. service-name-v3) */
export type ServiceSource = Brand<string, "ServiceSource">;

/** Branded schema hash identifier */
export type SchemaHash = Brand<string, "SchemaHash">;

/** Branded topic name for stronger compile-time guarantees */
export type TopicName = Brand<string, "TopicName">;

/** Branded consumer group id */
export type ConsumerGroupId = Brand<string, "ConsumerGroupId">;

/** Helper to brand a raw string as ServiceSource */
export const asServiceSource = (value: string): ServiceSource =>
  value as ServiceSource;
/** Helper to brand a raw string as TopicName */
export const asTopicName = (value: string): TopicName => value as TopicName;
/** Helper to brand a raw string as SchemaHash */
export const asSchemaHash = (value: string): SchemaHash => value as SchemaHash;
/** Helper to brand a raw string as ConsumerGroupId */
export const asConsumerGroupId = (value: string): ConsumerGroupId =>
  value as ConsumerGroupId;

export {};
