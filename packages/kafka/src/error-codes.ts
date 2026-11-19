import { defineErrorCodes } from "@gatwech/core/utils";

export const KAFKA_ERROR_CODES = defineErrorCodes({
	TOPIC_NAME_REQUIRED: "Topic name is required",
	TOPIC_NAME_DUPLICATED: "Topic name is already declared",
	TOPIC_SCHEMA_REQUIRED: "Topic schema is required",
	TOPIC_KEY_REQUIRED: "Topic key is required",
	INVALID_TOPIC_OPTION: "Invalid topic option",
	BROKERS_REQUIRED: "Brokers are required",
	INVALID_HEARTBEAT_INTERVAL:
		"Heartbeat interval must be shorter than the session timeout",
	INVALID_RETRY: "Retry back-off must be shorter than the session timeout",
	FAILED_TO_CONNECT: "Failed to connect to the brokers",
	CIRCUIT_OPEN: "Circuit breaker is open",
	TOPIC_NOT_PROVISIONED: "Topic is not provisioned",
	TOPIC_NOT_DECLARED: "Topic is not declared",
	INVALID_EVENT: "Invalid event",
	FAILED_TO_PUBLISH: "Failed to publish",
	FAILED_TO_START_CONSUMER: "Failed to start consumer",
	FAILED_TO_PUBLISH_TO_DLQ: "Failed to publish to the DLQ",
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
