import { z } from "@payvo/utils";
import type { Producer } from "kafkajs";
import { logger } from "../utils/logger";
import { BaseProducer } from "../client/base";
import type { TKafkaMessage } from "../types";
import { generateCorrelationId } from "../utils/helpers";

type KeyDeriver<T> = (payload: T) => string;

type StringifiableHeaders = Record<string, string | number | boolean | Date>;

interface TReadyToPublishMessage<T>
  extends Omit<TKafkaMessage<T>, "key" | "headers"> {
  key: string;
  headers?: StringifiableHeaders;
  metadata?: Record<string, unknown>;
}

class GenericKafkaProducer<
  TSchema extends z.ZodObject<any, any>,
> extends BaseProducer<z.infer<TSchema>> {
  protected readonly topic: string;
  private readonly keyDeriver: KeyDeriver<z.infer<TSchema>>;
  constructor(
    producer: Producer,
    topic: string,
    keyDeriver: KeyDeriver<z.infer<TSchema>>
  ) {
    super(producer);
    this.topic = topic;
    this.keyDeriver = keyDeriver;
  }

  private generateKey(payload: z.infer<TSchema>): string {
    return this.keyDeriver(payload);
  }

  protected enrichAndFormatMessage(
    payload: z.infer<TSchema>
  ): TReadyToPublishMessage<z.infer<TSchema>> {
    const key = this.generateKey(payload);

    const rawTimestamp = (payload as any).timestamp;
    const timestamp =
      rawTimestamp instanceof Date
        ? rawTimestamp
        : new Date(String(rawTimestamp));

    const metadata = (payload as any).metadata ?? {};
    const correlationId = metadata.correlationId;
    const schemaHash = metadata.schemaHash;
    const source = metadata.source;

    const headers: StringifiableHeaders = {
      "event-version": "1.0",
      "event-type": this.topic,
      timestamp: timestamp.toISOString(),
      ...(correlationId ? { "correlation-id": String(correlationId) } : {}),
      ...(source ? { source: String(source) } : {}),
      ...(schemaHash ? { "schema-hash": String(schemaHash) } : {}),
    };

    return {
      key,
      headers,
      value: payload,
    };
  }

  private stringifyHeaders(
    headers?: StringifiableHeaders
  ): Record<string, string> | undefined {
    if (!headers) return undefined;
    const result: Record<string, string> = {};
    for (const [k, v] of Object.entries(headers)) {
      result[k] = v instanceof Date ? v.toISOString() : String(v);
    }
    return result;
  }

  public async send(payload: z.infer<TSchema>): Promise<void> {
    const enriched = this.enrichAndFormatMessage(payload);
    const stringifiedHeaders = this.stringifyHeaders(enriched.headers);

    await this.publish({
      key: enriched.key,
      value: enriched.value,
      headers: stringifiedHeaders,
    } as TKafkaMessage<z.infer<TSchema>>);

    logger.info(`Published event to topic ${this.topic}`, {
      key: enriched.key,
      timestamp: (enriched.value as any).timestamp?.toString(),
    });
  }
}

/**
 * Factory for creating validated event publishers.
 */
export function createEventPublisher<TSchema extends z.ZodObject<any, any>>(
  producer: Producer,
  topicName: string,
  schema: TSchema,
  keyDeriver: KeyDeriver<z.infer<TSchema>>,
  source: string
) {
  const producerInstance = new GenericKafkaProducer<TSchema>(
    producer,
    topicName,
    keyDeriver
  );

  return async (payload: z.input<TSchema>): Promise<void> => {
    const validated = schema.parse(payload);

    const event: z.infer<TSchema> = {
      ...validated,
      metadata: {
        source,
        correlationId: generateCorrelationId("evt"),
        ...(validated.metadata ?? {}),
      },
    };

    await producerInstance.send(event);
  };
}
