import { z } from "@gatwech/utils";
import type { Producer } from "kafkajs";
import { generateCorrelationId } from "../../utils/helpers";
import { createEventPublisher } from "./event.factory";
import type { PartitionKeyFn } from "./generic-producer";
import type { EventMetadata } from "../../utils";
import type { ServiceSource } from "../../types/utility-types";

/**
 * Function signature exposed to service/business code for publishing a validated event.
 */
export type EventPublisher<TSchema extends z.ZodObject<any, any>> = (
	payload: z.input<TSchema>,
) => Promise<void>;

/**
 * Create a service-level factory that encapsulates shared producer and source metadata.
 * @param producer Shared KafkaJS producer instance.
 * @param serviceSource Stable identifier for the emitting service (e.g. "user-service-v3").
 */
export function createServicePublisherFactory(
	producer: Producer,
	serviceSource: ServiceSource | string,
) {
	/**
	 * Define a concrete event publisher for a topic + schema + key derivation.
	 * @param topic Kafka topic name for this event type.
	 * @param schema Zod schema validating the payload shape.
	 * @param getPartitionKey Function deriving the partitioning key from the validated payload.
	 */
	return function defineEvent<TSchema extends z.ZodObject<any, any>>(
		topic: string,
		schema: TSchema,
		getPartitionKey: PartitionKeyFn<z.infer<TSchema>>,
	): EventPublisher<TSchema> {
		const publisherInstance = createEventPublisher(
			producer,
			topic,
			schema,
			getPartitionKey,
			serviceSource,
		);

		return async (payload: z.input<TSchema>): Promise<void> => {
			const base = payload as Record<string, unknown>;
			const existingMeta = (base.metadata || {}) as Partial<EventMetadata>;

			const enriched = {
				...base,
				timestamp: typeof base.timestamp === "number" ? base.timestamp : Date.now(),
				metadata: {
					source: serviceSource as ServiceSource,
					correlationId: existingMeta.correlationId || generateCorrelationId("evt"),
					...existingMeta,
				},
			} as z.input<TSchema>;

			return publisherInstance(enriched);
		};
	};
}
