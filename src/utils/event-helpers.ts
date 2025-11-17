import { logger } from "./logger";
import type {
  CorrelationId,
  ServiceSource,
  SchemaHash,
  TopicName,
} from "../types/utility-types";

/** Metadata fields attached to events for tracing and schema info. */
export type EventMetadata = {
  correlationId?: CorrelationId;
  source?: ServiceSource | string;
  schemaHash?: SchemaHash | string;
};

/** Headers that can be converted to strings before sending to Kafka. */
export type StringifiableHeaders = Record<
  string,
  string | number | boolean | Date
>;

// Timestamp parsing helper removed: producers now require epoch milliseconds (number)
// Any legacy events with string/Date timestamps should be normalized upstream before publishing.

/**
 * Safely extract event metadata from an unknown value.
 * - Accepts plain objects, ignores arrays and primitives
 * - Returns an empty object when invalid
 */
export function extractEventMetadata(value: unknown): EventMetadata {
  const metaCandidate = value as
    | EventMetadata
    | Record<string, unknown>
    | undefined;
  const metadata: EventMetadata =
    metaCandidate &&
    typeof metaCandidate === "object" &&
    !Array.isArray(metaCandidate)
      ? (metaCandidate as EventMetadata)
      : {};
  return metadata;
}

/**
 * Build standard event headers for Kafka messages.
 * Includes version, type (topic), timestamp, and optional correlation and schema info.
 */
export function buildHeaders(
  topic: TopicName | string,
  timestamp: Date,
  metadata: EventMetadata
): StringifiableHeaders {
  const correlationId =
    typeof metadata.correlationId === "string"
      ? metadata.correlationId
      : undefined;
  const schemaHash =
    typeof metadata.schemaHash === "string"
      ? metadata.schemaHash
      : (metadata.schemaHash as SchemaHash | undefined);
  const source =
    typeof metadata.source === "string"
      ? metadata.source
      : (metadata.source as ServiceSource | undefined);

  return {
    "event-version": "1.0",
    "event-type": topic,
    timestamp: timestamp.toISOString(),
    ...(correlationId ? { "correlation-id": String(correlationId) } : {}),
    ...(source ? { source: String(source) } : {}),
    ...(schemaHash ? { "schema-hash": String(schemaHash) } : {}),
  };
}

/**
 * Convert header values to strings for KafkaJS producer.
 * Dates are ISO-stringified, other primitives are String()-cast.
 */
export function stringifyHeaders(
  headers?: StringifiableHeaders
): Record<string, string> | undefined {
  if (!headers) return undefined;
  const result: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) {
    result[k] = v instanceof Date ? v.toISOString() : String(v);
  }
  return result;
}
