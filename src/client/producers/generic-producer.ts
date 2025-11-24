import { z } from "@gatwech/utils";
import type { Producer } from "kafkajs";
import { BaseProducer } from "./base";
import type { TKafkaMessage } from "../../types";
import type { DeepReadonly, StrictOmit } from "../../types/utility-types";
import { logger } from "../../utils/logger";
import {
  extractEventMetadata,
  buildHeaders,
  stringifyHeaders,
  type EventMetadata,
  type StringifiableHeaders,
} from "../../utils";

/**
 * @deprecated Use `PartitionKeyFn<T>` instead. This alias will be removed in a future release.
 */
export type KeyDeriver<T> = (payload: DeepReadonly<T>) => string;

/** Preferred: function that derives a partition key from the payload. */
export type PartitionKeyFn<T> = (payload: DeepReadonly<T>) => string;

type BaseEventFields = {
  timestamp: number;
  metadata?: EventMetadata | Record<string, unknown>;
};

export type ReadyToPublishMessage<T> = StrictOmit<
  TKafkaMessage<T>,
  "key" | "headers"
> & {
  key: string;
  headers?: StringifiableHeaders;
  metadata?: Record<string, unknown>;
};

export class GenericKafkaProducer<
  TSchema extends z.ZodObject<any, any>
> extends BaseProducer<z.infer<TSchema>> {
  protected readonly topic: string;
  private readonly partitionKey: PartitionKeyFn<z.infer<TSchema>>;
  constructor(
    producer: Producer,
    topic: string,
    partitionKey: PartitionKeyFn<z.infer<TSchema>>
  ) {
    super(producer);
    this.topic = topic;
    this.partitionKey = partitionKey;
  }

  private generateKey(
    payload: DeepReadonly<z.infer<TSchema>>,
    overrideKey?: string
  ): string {
    if (overrideKey) return overrideKey;
    return this.partitionKey(payload);
  }

  protected enrichAndFormatMessage(
    payload: DeepReadonly<z.infer<TSchema>>,
    overrideKey?: string
  ): ReadyToPublishMessage<z.infer<TSchema>> {
    const key = this.generateKey(payload, overrideKey);

    const withBase = payload as z.infer<TSchema> & BaseEventFields;
    const tsValue = withBase.timestamp;
    const timestamp = tsValue ? new Date(tsValue) : new Date();
    if (isNaN(timestamp.getTime())) {
      throw new Error(
        `Invalid timestamp value for payload: ${JSON.stringify(payload)}`
      );
    }

    const metadata = extractEventMetadata(withBase.metadata);
    const headers = buildHeaders(this.topic, timestamp, metadata);

    return {
      key,
      headers,
      value: payload as unknown as z.infer<TSchema>,
    };
  }

  public async send(payload: DeepReadonly<z.infer<TSchema>>): Promise<void> {
    const enriched = this.enrichAndFormatMessage(payload);
    const stringifiedHeaders = stringifyHeaders(enriched.headers);

    await this.publish({
      key: enriched.key,
      value: enriched.value,
      headers: stringifiedHeaders,
    } as TKafkaMessage<DeepReadonly<z.infer<TSchema>>>);

    logger.info(`Published event to topic ${this.topic}`, {
      key: enriched.key,
      timestamp: stringifiedHeaders?.timestamp,
    });
  }

  /**
   * Efficient batch send: enrich all payloads then publish in a single kafka send.
   * Each item provides its explicit key (overrides key derivation).
   */
  public async sendBatch(
    items: { payload: DeepReadonly<z.infer<TSchema>>; key: string }[]
  ): Promise<void> {
    const ready = items.map((itm) =>
      this.enrichAndFormatMessage(itm.payload, itm.key)
    );
    const kafkaMessages: TKafkaMessage<DeepReadonly<z.infer<TSchema>>>[] =
      ready.map((m) => ({
        key: m.key,
        value: m.value as DeepReadonly<z.infer<TSchema>>,
        headers: stringifyHeaders(m.headers),
      }));
    await this.publishBatch(kafkaMessages);
    logger.info(`Published batch to ${this.topic}`, {
      count: kafkaMessages.length,
    });
  }
}
