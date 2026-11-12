import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";
import type { Topic } from "../topics";
import type { CorrelationId } from "../types/utility-types";
import {
	buildHeaders,
	extractEventMetadata,
	stringifyHeaders,
} from "../utils/event-helpers";
import { generateCorrelationId } from "../utils/helpers";

/**
 * The messages for events of one topic, ready for the broker
 * @description Each event is checked by the topic's schema and keyed by the
 * topic's `key`. The message has the shape `createEventPublisher` writes, so
 * consumers that have not moved read it unchanged: the body is what the schema
 * outputs with `metadata: { source, correlationId }` added, and the headers
 * are `event-version`, `event-type`, `timestamp`, `correlation-id` and
 * `source`. Throws `EVENT_REJECTED_BY_SCHEMA` for the first event the schema
 * rejects, before any message is built for the ones after it
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
	const generatedCorrelationId = generateCorrelationId("evt");

	return options.events.map((event) => {
		const parsed = options.topic.schema.safeParse(event);
		if (!parsed.success) {
			throw new KafkaError(
				KAFKA_ERROR_CODES.EVENT_REJECTED_BY_SCHEMA,
				parsed.error,
			);
		}

		const data = parsed.data as Record<string, unknown>;
		const ownMetadata = extractEventMetadata(data.metadata);
		const metadata = {
			source: options.clientId,
			...ownMetadata,
			correlationId: (options.correlationId ??
				ownMetadata.correlationId ??
				generatedCorrelationId) as CorrelationId,
		};
		const timestamp =
			typeof data.timestamp === "number" ? new Date(data.timestamp) : new Date();

		return {
			key: options.topic.key(parsed.data),
			value: JSON.stringify({ ...data, metadata }),
			headers: {
				...stringifyHeaders(buildHeaders(options.topic.name, timestamp, metadata)),
				...options.headers,
			},
		};
	});
}
