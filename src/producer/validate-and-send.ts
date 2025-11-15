import { z } from "@gatwech/utils";
import { logger } from "../utils/logger";
import type { TKafkaMessage } from "../types";
import type { TopicConfig } from "../config/topics";
import { KafkaClient, KafkaError } from "../client/kafka-client";

/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
export async function validateAndSend<T>(
  client: KafkaClient,
  topic: TopicConfig,
  message: TKafkaMessage<T>
): Promise<void> {
  if (!topic.schema) {
    logger.warn("No schema defined for topic; not validating message", {
      topic: topic.name,
    });
    return client.sendMessage(topic.name, message);
  }

  const schema = topic.schema as z.ZodType<T>;
  const res = schema.safeParse(message.value);
  if (!res.success) {
    const tree = z.treeifyError(res.error as any);
    logger.warn("Producer validation failed; not sending message", {
      topic: topic.name,
      errors: tree,
    });
    throw new KafkaError("Validation failed for message", res.error);
  }

  return client.sendMessage(topic.name, message);
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
  for (const msg of messages) {
    const res = schema.safeParse(msg.value);
    if (!res.success) {
      const tree = z.treeifyError(res.error as any);
      logger.warn("Producer batch validation failed; not sending batch", {
        topic: topic.name,
        errors: tree,
      });
      throw new KafkaError("Validation failed for batch message", res.error);
    }
  }

  return client.sendMessages(topic.name, messages);
}

export default { validateAndSend, validateAndSendMany };
