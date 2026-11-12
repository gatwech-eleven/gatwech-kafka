import type { z } from "@gatwech/utils/zod";
import type { Topic } from "../topics/types";

/**
 * What `kafka.publish` takes
 * @description The message has the shape `createEventPublisher` writes today,
 * so consumers that have not moved yet read it unchanged: the body is the event
 * with `metadata: { source, correlationId }` added, and the headers are
 * `event-version`, `event-type` (the topic's name on the broker), `timestamp`
 * and `correlation-id`. `source` is the client's `clientId`
 */
export interface PublishOptions<
	Name extends string = string,
	T extends Topic<any> = Topic,
> {
	/**
	 * The key of the topic in the `topics` option of `createKafka`
	 * @description The topic's schema checks the event, and its `key` picks the
	 * partition
	 *
	 * @example "userRegistered"
	 */
	topic: Name;
	/**
	 * The event to publish
	 * @description The publish is refused with `EVENT_REJECTED_BY_SCHEMA` when
	 * the topic's schema rejects it
	 */
	event: z.input<T["schema"]>;
	/**
	 * Ties this message to the others of one request
	 * @description Consumers get it as `ctx.correlationId`
	 *
	 * @default a new id for each publish
	 */
	correlationId?: string | undefined;
	/**
	 * Extra headers on the message
	 */
	headers?: Record<string, string> | undefined;
}

export interface PublishBatchOptions<
	Name extends string = string,
	T extends Topic<any> = Topic,
> {
	/**
	 * The key of the topic in the `topics` option of `createKafka`
	 *
	 * @example "userRegistered"
	 */
	topic: Name;
	/**
	 * The events to publish, in one request to the broker
	 * @description Each one is checked first, and none is sent if any is
	 * rejected
	 */
	events: z.input<T["schema"]>[];
	/**
	 * Ties these messages to the others of one request
	 *
	 * @default a new id for each publish
	 */
	correlationId?: string | undefined;
	/**
	 * Extra headers on every message
	 */
	headers?: Record<string, string> | undefined;
}
