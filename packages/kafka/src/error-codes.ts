import { defineErrorCodes } from "@gatwech/core/utils";

export const KAFKA_ERROR_CODES = defineErrorCodes({
	TOPIC_NAME_REQUIRED: "A topic needs a name",
	TOPIC_NAME_DUPLICATED:
		"Two topics have the same name. Give them different names, or set namespaced: false on one",
	TOPIC_SCHEMA_REQUIRED: "A topic needs a schema, such as z.object({ ... })",
	TOPIC_KEY_REQUIRED:
		"A topic needs a key function that returns the partition key of an event",
	TOPIC_OPTION_INVALID:
		"partitions, replicationFactor and retentionMs must be positive whole numbers",
	BROKERS_REQUIRED: "options.brokers must list at least one broker",
	HEARTBEAT_NOT_BELOW_SESSION_TIMEOUT:
		"session.heartbeatIntervalMs must be shorter than session.timeoutMs",
	RETRY_EXCEEDS_SESSION_TIMEOUT:
		"The retries back off for longer than session.timeoutMs, so the broker would remove the consumer from its group. Lower retry.attempts or retry.backoffMs, or raise session.timeoutMs",
	BROKER_UNREACHABLE: "No broker answered within connect.attempts tries",
	NOT_CONNECTED: "Call connect() before publishing or consuming",
	CIRCUIT_OPEN:
		"The circuit breaker is open: publishes are failing, and the next one is let through after circuitBreaker.resetTimeoutMs",
	TOPIC_NOT_PROVISIONED:
		"The topic doesn't exist, and topics are never created on first use. Call provision() with it first",
	TOPIC_NOT_DECLARED:
		"The client has no topic with that key. Declare it in the topics option of createKafka",
	EVENT_REJECTED_BY_SCHEMA: "The event doesn't match the topic's schema",
	PUBLISH_FAILED: "The broker did not accept the message",
	CONSUMER_START_FAILED:
		"The consumer could not connect, join its group or subscribe to the topic",
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
