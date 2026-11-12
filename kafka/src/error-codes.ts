import { defineErrorCodes } from "@gatwech/core/utils";

export const KAFKA_ERROR_CODES = defineErrorCodes({
	BROKERS_REQUIRED: "options.brokers must list at least one broker",
	HEARTBEAT_NOT_BELOW_SESSION_TIMEOUT:
		"session.heartbeatInterval must be shorter than session.timeout",
	RETRY_EXCEEDS_SESSION_TIMEOUT:
		"The retries back off for longer than session.timeout, so the broker would remove the consumer from its group. Lower retry.attempts or retry.backoffMs, or raise session.timeout",
	BROKER_UNREACHABLE: "No broker answered within connect.maxAttempts attempts",
	NOT_CONNECTED: "Call connect() before publishing or consuming",
	CIRCUIT_OPEN:
		"The circuit breaker is open: publishes are failing, and the next one is let through after circuitBreaker.resetTimeoutMs",
	TOPIC_NOT_PROVISIONED:
		"The topic doesn't exist, and topics are never created on first use. Call provision() with it first",
	EVENT_REJECTED_BY_SCHEMA: "The event doesn't match the topic's schema",
	DLQ_PUBLISH_FAILED:
		"The message could not be published to the DLQ, so its partition was paused and its offset left uncommitted",
});

/**
 * The one error this package throws
 *
 * `message` is one of {@link KAFKA_ERROR_CODES}. `cause` is the error behind
 * it: the broker's, the handler's or the schema's.
 */
export class KafkaError extends Error {
	constructor(message: string, cause?: unknown | undefined) {
		super(message, { cause });
		this.name = "KafkaError";
	}
}
