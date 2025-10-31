import { z } from 'zod';
import { l as logger } from './kafka.Dv3r60xd.mjs';
import { K as KafkaError } from './kafka.D0nFqeDt.mjs';

async function validateAndSend(client, topic, schema, message) {
  const res = schema.safeParse(message.value);
  if (!res.success) {
    const tree = z.treeifyError(res.error);
    logger.warn("Producer validation failed; not sending message", {
      topic,
      errors: tree
    });
    throw new KafkaError("Validation failed for message", res.error);
  }
  return client.sendMessage(topic, message);
}
async function validateAndSendMany(client, topic, schema, messages) {
  for (const msg of messages) {
    const res = schema.safeParse(msg.value);
    if (!res.success) {
      const tree = z.treeifyError(res.error);
      logger.warn("Producer batch validation failed; not sending batch", {
        topic,
        errors: tree
      });
      throw new KafkaError("Validation failed for batch message", res.error);
    }
  }
  return client.sendMessages(topic, messages);
}
const validateAndSend$1 = { validateAndSend, validateAndSendMany };

const validateAndSend$2 = {
  __proto__: null,
  default: validateAndSend$1,
  validateAndSend: validateAndSend,
  validateAndSendMany: validateAndSendMany
};

export { validateAndSend as a, validateAndSendMany as b, validateAndSend$1 as c, validateAndSend$2 as v };
