import { nanoid } from "nanoid";
import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import type { Topic } from "../topics";

/**
 * The messages for events of one topic, ready for the broker
 * @description Each event is checked by the topic's schema and keyed by the
 * topic's `key`. The message has the shape `createEventPublisher` writes, so
 * consumers that have not moved read it unchanged: the body is what the schema
 * outputs with `metadata: { source, correlationId }` added, and the headers
 * are `event-version`, `event-type`, `timestamp`, `correlation-id`, `source`
 * and, when the event's metadata carries one, `schema-hash`. Throws
 * `EVENT_REJECTED_BY_SCHEMA` for the first event the schema rejects, before
 * any message is built for the ones after it
 */
export function buildMessages(options: {
	/**
	 * The topic as the broker knows it
	 */
	topic: Topic<any>;
	/**
	 * The `clientId`, which the messages carry as their `source`
	 */
	clientId: string;
	events: unknown[];
	correlationId?: string | undefined;
	headers?: Record<string, string> | undefined;
}) {
	const generatedCorrelationId = `evt_${nanoid(12)}`;

	return options.events.map((event) => {
		const parsed = options.topic.schema.safeParse(event);
		if (!parsed.success) {
			throw new KafkaError(
				KAFKA_ERROR_CODES.EVENT_REJECTED_BY_SCHEMA,
				parsed.error,
			);
		}

		const data = parsed.data as Record<string, unknown>;
		const ownMetadata =
			typeof data.metadata === "object" &&
			data.metadata !== null &&
			!Array.isArray(data.metadata)
				? (data.metadata as Record<string, unknown>)
				: {};
		const metadata = {
			source: options.clientId,
			...ownMetadata,
			correlationId:
				options.correlationId ??
				(typeof ownMetadata.correlationId === "string"
					? ownMetadata.correlationId
					: undefined) ??
				generatedCorrelationId,
		};
		const timestamp =
			typeof data.timestamp === "number" ? new Date(data.timestamp) : new Date();

		return {
			key: options.topic.key(parsed.data),
			value: JSON.stringify({ ...data, metadata }),
			headers: {
				"event-version": "1.0",
				"event-type": options.topic.name,
				timestamp: timestamp.toISOString(),
				"correlation-id": metadata.correlationId,
				...(typeof metadata.source === "string" ? { source: metadata.source } : {}),
				...(typeof ownMetadata.schemaHash === "string"
					? { "schema-hash": ownMetadata.schemaHash }
					: {}),
				...options.headers,
			},
		};
	});
}
