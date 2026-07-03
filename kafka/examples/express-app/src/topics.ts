import { z } from '@gatwech/utils/zod';
import { createTopicConfig } from '@gatwech/kafka';

export const AppTopics = createTopicConfig({
  USER_EVENTS: {
    name: 'user.events',
    schema: z.object({
      userId: z.string(),
      event: z.string(),
      timestamp: z.number(),
    }),
    partitions: 3,
    retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 days
    cleanupPolicy: 'delete',
  },
  ORDER_EVENTS: {
    name: 'order.events',
    schema: z.object({
      orderId: z.string(),
      userId: z.string(),
      amount: z.number(),
      status: z.string(),
      timestamp: z.number(),
    }),
    partitions: 6,
    retentionMs: 30 * 24 * 60 * 60 * 1000, // 30 days
    cleanupPolicy: 'delete',
  },
});
