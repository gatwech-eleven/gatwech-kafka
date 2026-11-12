import type { z } from "@gatwech/utils/zod";
import type { Topic } from "../topics/types";

export interface PublisherOptions<Name extends string = string> {
	/**
	 * The key of the topic in the `topics` option of `createKafka`
	 * @description The topic's schema checks every event, and its `key` picks
	 * the partition
	 *
	 * @example "userRegistered"
	 */
	topic: Name;
	/**
	 * The service publishing, recorded in each message's metadata
	 *
	 * @example "payvo-auth-service"
	 */
	source: string;
}

export interface PublishOptions<T extends Topic<any> = Topic> {
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

export interface PublishBatchOptions<T extends Topic<any> = Topic> {
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

export interface PublishResult {
	/**
	 * The partition the message went to
	 */
	partition: number;
	/**
	 * The offset the broker gave the message. For a batch, the first one's
	 */
	offset: string;
}

export interface Publisher<T extends Topic<any> = Topic> {
	/**
	 * Publishes one event
	 *
	 * @example
	 * await userRegistered.publish({ event: { userId, email } });
	 */
	publish: (options: PublishOptions<T>) => Promise<PublishResult>;
	/**
	 * Publishes several events in one request
	 */
	publishBatch: (options: PublishBatchOptions<T>) => Promise<PublishResult>;
}
