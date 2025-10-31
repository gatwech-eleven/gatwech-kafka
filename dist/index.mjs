export { B as BaseProducer, c as createEventPublisher } from './shared/kafka.BLS31-Pb.mjs';
export { a as KafkaClient, K as KafkaError } from './shared/kafka.D0nFqeDt.mjs';
export { c as connectWithRetry } from './shared/kafka.6TvAvwPb.mjs';
export { chatTopics, notificationTopics, orderTopics, paymentTopics, topics, userTopics } from './config/topics.mjs';
export { ensureTopicsWithConfig } from './config/ensure-topics.mjs';
export { JsonSerializer } from './types.mjs';
export { i as validation } from './shared/kafka.BjSyh3IS.mjs';
export { v as producer } from './shared/kafka.DbdJYucb.mjs';
export { i as schemas } from './shared/kafka.CBrliBar.mjs';
import { z } from '@payvo/utils/zod';
import './shared/kafka.Dv3r60xd.mjs';
import 'winston';
import 'nanoid';
import 'kafkajs';
import 'zod';

function resolveBrokersFromEnv() {
  const raw = process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS;
  if (raw && raw.trim().length > 0) {
    return raw.split(",").map((s) => s.trim()).filter(Boolean);
  }
  if (process.env.KAFKA_PREFER_DOCKER_HOSTNAME === "1") {
    return ["kafka:9092"];
  }
  return ["localhost:9092"];
}

const BaseProducerSchema = z.object({
  timestamp: z.date().optional().default(() => /* @__PURE__ */ new Date()),
  metadata: z.any().optional()
});

export { BaseProducerSchema, resolveBrokersFromEnv };
