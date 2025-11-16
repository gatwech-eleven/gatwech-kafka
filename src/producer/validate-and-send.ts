import { z } from "@gatwech/utils";
import { logger } from "../utils/logger";
import type { TKafkaMessage } from "../types";
import type { TopicConfig } from "../config/topics";
import { KafkaClient, KafkaError } from "../client/kafka";
import { GenericKafkaProducer } from "../client/generic-producer";

/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
export async function validateAndSend<T>(
  client: KafkaClient,
  topic: TopicConfig,
  message: TKafkaMessage<T>
): Promise<void> {
  // Fallback: no schema -> delegate directly (legacy path)
  if (!topic.schema) {
    logger.warn("No schema defined for topic; not validating message", {
      topic: topic.name,
    });
    return client.sendMessage(topic.name, message);
  }

  const schema = topic.schema as z.ZodType<T>;
  const parsed = schema.safeParse(message.value);
  if (!parsed.success) {
    const tree = z.treeifyError(parsed.error as any);
    logger.warn("Producer validation failed; not sending message", {
      topic: topic.name,
      errors: tree,
    });
    throw new KafkaError("Validation failed for message", parsed.error);
  }

  const producer = client.getProducer();
  let currentKey = message.key;
  const generic = new GenericKafkaProducer<any>(
    producer,
    topic.name,
    () => currentKey
  );

  // Ensure timestamp field exists for header building.
  const payloadWithTimestamp = {
    ...(parsed.data as any),
    timestamp: (parsed.data as any)?.timestamp || Date.now(),
    metadata: (parsed.data as any)?.metadata,
  } as T & { timestamp: number };

  await generic.send(payloadWithTimestamp as any);
}

/** Validate and send multiple messages. Any invalid message will abort sending. */
export async function validateAndSendMany<T>(
  client: KafkaClient,
  topic: TopicConfig,
  messages: TKafkaMessage<T>[]
): Promise<void> {
  if (!topic.schema) {
    logger.warn("No schema defined for topic; not validating messages", {
      topic: topic.name,
    });
    return client.sendMessages(topic.name, messages);
  }

  const schema = topic.schema as z.ZodType<T>;
  const producer = client.getProducer();
  let currentKey = "";
  const generic = new GenericKafkaProducer<any>(
    producer,
    topic.name,
    () => currentKey
  );

  for (const msg of messages) {
    const res = schema.safeParse(msg.value);
    logger.info("Batch validation result", {
      key: msg.key,
      success: res.success,
      valueKeys: Object.keys(msg.value || {}),
    });
    if (!res.success) {
      // Log early to guarantee visibility even if treeifyError throws
      logger.warn("Producer batch validation failed; not sending batch", {
        topic: topic.name,
      });
      let tree: unknown;
      try {
        tree = z.treeifyError(res.error as any);
        logger.warn("Batch validation errors", {
          topic: topic.name,
          errors: tree,
        });
      } catch (e) {
        logger.warn("treeifyError failed", {
          topic: topic.name,
          error: (e as any)?.message,
        });
      }
      throw new KafkaError("Validation failed for batch message", res.error);
    }
  }
  // Enrich payloads (timestamp & metadata) and publish as a batch for efficiency.
  const batchItems = messages.map((msg) => {
    const enriched = {
      ...(msg.value as any),
      timestamp: (msg.value as any)?.timestamp || Date.now(),
      metadata: (msg.value as any)?.metadata,
    } as T & { timestamp: number };
    return { key: msg.key, payload: enriched };
  });

  await generic.sendBatch(batchItems as any);
}

export default { validateAndSend, validateAndSendMany };
