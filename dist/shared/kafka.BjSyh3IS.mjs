import { z } from 'zod';
import { l as logger } from './kafka.Dv3r60xd.mjs';

function validateMessage(schema, value, context) {
  const res = schema.safeParse(value);
  if (res.success) return { success: true, data: res.data };
  const tree = z.treeifyError(res.error);
  logger.warn("Kafka message validation failed", {
    ...context || {},
    errors: tree
  });
  return { success: false, error: tree };
}
const index = { validateMessage };

const index$1 = {
  __proto__: null,
  default: index,
  validateMessage: validateMessage
};

export { index as a, index$1 as i, validateMessage as v };
