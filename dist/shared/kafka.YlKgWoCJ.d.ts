import { z } from 'zod';

type ValidationResult<T> = {
    success: true;
    data: T;
} | {
    success: false;
    error: ReturnType<typeof z.treeifyError>;
};
declare function validateMessage<T>(schema: z.ZodType<T>, value: unknown, context?: Record<string, unknown>): ValidationResult<T>;
declare const _default: {
    validateMessage: typeof validateMessage;
};

type index_ValidationResult<T> = ValidationResult<T>;
declare const index_validateMessage: typeof validateMessage;
declare namespace index {
  export { _default as default, index_validateMessage as validateMessage };
  export type { index_ValidationResult as ValidationResult };
}

export { _default as _, index as i, validateMessage as v };
export type { ValidationResult as V };
