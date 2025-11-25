import { describe, it, expect, vi } from 'vitest';
import { KafkaClient } from '../../../src/client/core/kafka';
import { createTopicConfig } from '../../../src/config/topics/define-topics';
import { z } from '@gatwech/utils/zod';

// Mock KafkaJS
vi.mock('kafkajs', async () => {
  const actual = await vi.importActual('kafkajs');
  return {
    ...actual,
    Kafka: vi.fn(function() {
      return {
        producer: vi.fn().mockReturnValue({
          connect: vi.fn(),
          disconnect: vi.fn(),
          on: vi.fn(),
          send: vi.fn().mockResolvedValue(undefined),
        }),
        admin: vi.fn().mockReturnValue({
          connect: vi.fn(),
          disconnect: vi.fn(),
        }),
      };
    }),
  };
});

describe('Client Topics Integration', () => {
  it('should store topics from options', () => {
    const topics = createTopicConfig({
      USER_CREATED: {
        name: 'user.created',
        schema: z.object({ id: z.string() }),
      },
      ORDER_PLACED: {
        name: 'order.placed',
        schema: z.object({ orderId: z.string() }),
      },
    });

    const client = new KafkaClient('test-client', ['localhost:9092'], {
      topics,
    });

    expect(client.topics).toBeDefined();
    expect(client.topics?.USER_CREATED).toBeDefined();
    expect(client.topics?.USER_CREATED.name).toBe('user.created');
    expect(client.topics?.ORDER_PLACED.name).toBe('order.placed');
  });

  it('should return undefined when no topics configured', () => {
    const client = new KafkaClient('test-client', ['localhost:9092']);
    expect(client.topics).toBeUndefined();
  });

  it('should provide readonly access to topics', () => {
    const topics = createTopicConfig({
      TEST_TOPIC: {
        name: 'test.topic',
        schema: z.object({ data: z.string() }),
      },
    });

    const client = new KafkaClient('test-client', ['localhost:9092'], {
      topics,
    });

    // TypeScript should prevent modification
    // @ts-expect-error - topics should be readonly
    expect(() => { client.topics = {}; }).toThrow();
  });
});
