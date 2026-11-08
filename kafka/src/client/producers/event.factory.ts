import { z } from "@gatwech/utils";
import type { Producer } from "kafkajs";
import { type EventMetadata } from "../../utils";
import { GenericKafkaProducer, type PartitionKeyFn } from "./generic-producer";
import type { DeepReadonly } from "../../types/utility-types";
import { generateCorrelationId } from "../../utils/helpers";

/**
 * Factory for creating validated event publishers.
 * @param producer The KafkaJS Producer instance to use for sending messages.
 * @param topic The name of the Kafka topic to publish events to.
 * @param schema A Zod schema object for validating event payloads.
 * @param key A function that derives the message key from the event payload.
 *            Prefer naming this function `getPartitionKey` at the call site for clarity.
 * @param source A string indicating the source of the events (included in metadata).
 * @returns A function that accepts a payload, validates it, enriches it with metadata, and publishes it to Kafka.
 */
export function createEventPublisher<TSchema extends z.ZodObject<any, any>>(
	producer: Producer,
	topic: string,
	schema: TSchema,
	partitionKey: PartitionKeyFn<z.infer<TSchema>>,
	source: string,
) {
	const producerInstance = new GenericKafkaProducer<TSchema>(
		producer,
		topic,
		partitionKey,
	);

	return async (payload: z.input<TSchema>): Promise<void> => {
		const validated = schema.parse(payload);

		const rawMeta = (validated as Partial<{ metadata?: unknown }>)?.metadata;
		const safeMeta: EventMetadata =
			rawMeta && typeof rawMeta === "object" && !Array.isArray(rawMeta)
				? (rawMeta as EventMetadata)
				: {};

		const event = {
			...(validated as Record<string, unknown>),
			metadata: {
				source,
				correlationId: generateCorrelationId("evt"),
				...safeMeta,
			},
		} as z.infer<TSchema>;

		await producerInstance.send(event as DeepReadonly<z.infer<TSchema>>);
	};
}

/** Options for `createPublisher` convenience helper */
export type CreatePublisherOptions<TSchema = any> = {
	producer: Producer;
	topic: string;
	source?: string;
	/** key must be a function deriving a key from the payload */
	key: (p: z.input<TSchema>) => string;
	metadata?: Record<string, unknown>;
};

/**
 * Flexible helper (requires a key-deriver function):
 * - `createPublisher(opts, schema)` -> returns `publisher(payload)`
 * - `createPublisher(opts)(schema)` -> returns `publisher(payload)`
 *
 * Where `opts.key` is `(payload) => string`, typically extracting a stable
 * partitioning key such as `userId` or `email` from the payload.
 */
export function createPublisher<TSchema extends z.ZodObject<any, any>>(
	opts: CreatePublisherOptions<TSchema>,
	schema?: TSchema,
) {
	const make = (s: TSchema) => {
		const partitionKey: PartitionKeyFn<z.infer<TSchema>> = (p) =>
			opts.key(p as unknown as z.input<TSchema>);

		const publisher = createEventPublisher(
			opts.producer,
			opts.topic,
			s,
			partitionKey,
			opts.source ?? "unknown-service",
		);

		return async (payload: z.input<TSchema>) => {
			const merged = {
				...(payload as Record<string, unknown>),
				metadata: {
					...(payload as any)?.metadata,
					...(opts.metadata || {}),
				},
			} as z.input<TSchema>;

			return publisher(merged);
		};
	};

	if (schema) return make(schema);

	return (s: TSchema) => make(s);
}
