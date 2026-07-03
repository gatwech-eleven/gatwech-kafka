import { nanoid } from "nanoid";
import type { CorrelationId } from "../types/utility-types";

export const CORRELATION_ID_LENGTH = 12;

/**
 * Generates a correlation ID with optional prefix
 * @param prefix - Optional prefix for human readability
 * @returns Formatted correlation ID (e.g., "reg_AbCdEf123")
 */
export const generateCorrelationId = (prefix?: string): CorrelationId => {
  const id = nanoid(CORRELATION_ID_LENGTH);
  const raw = prefix ? `${prefix}_${id}` : id;
  return raw as CorrelationId;
};
