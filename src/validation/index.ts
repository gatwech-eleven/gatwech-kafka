import { z } from "zod";
import { logger } from "../utils/logger";

export type ValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: ReturnType<typeof z.treeifyError> };

export function validateMessage<T>(
  schema: z.ZodType<T>,
  value: unknown,
  context?: Record<string, unknown>
): ValidationResult<T> {
  const res = schema.safeParse(value);
  if (res.success) return { success: true, data: res.data };

  const tree = z.treeifyError(res.error as any);
  logger.warn("Kafka message validation failed", {
    ...(context || {}),
    errors: tree,
  });
  return { success: false, error: tree };
}

export default { validateMessage };
