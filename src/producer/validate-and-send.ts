import { z } from "@gatwech/utils";
import type { TKafkaMessage } from "../types";
import { logger } from "../utils/logger";
import { KafkaClient, KafkaError } from "../client/kafka-client";

/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
export async function validateAndSend<T>(
  client: KafkaClient,
  topic: string,
  schema: z.ZodType<T>,
  message: TKafkaMessage<T>
): Promise<void> {
  const res = schema.safeParse(message.value);
  if (!res.success) {
    const tree = z.treeifyError(res.error as any);
    logger.warn("Producer validation failed; not sending message", {
      topic,
      errors: tree,
    });
    throw new KafkaError("Validation failed for message", res.error);
  }

  return client.sendMessage(topic, message);
}

/** Validate and send multiple messages. Any invalid message will abort sending. */
export async function validateAndSendMany<T>(
  client: KafkaClient,
  topic: string,
  schema: z.ZodType<T>,
  messages: TKafkaMessage<T>[]
): Promise<void> {
  for (const msg of messages) {
    const res = schema.safeParse(msg.value);
    if (!res.success) {
      const tree = z.treeifyError(res.error as any);
      logger.warn("Producer batch validation failed; not sending batch", {
        topic,
        errors: tree,
      });
      throw new KafkaError("Validation failed for batch message", res.error);
    }
  }

  return client.sendMessages(topic, messages);
}

export default { validateAndSend, validateAndSendMany };
