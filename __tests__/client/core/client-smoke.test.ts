import { describe, it, expect, vi, beforeEach } from 'vitest';
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
          connect: vi.fn().mockResolvedValue(undefined),
          disconnect: vi.fn().mockResolvedValue(undefined),
          on: vi.fn(),
          send: vi.fn().mockResolvedValue(undefined),
          transaction: vi.fn().mockResolvedValue({
            send: vi.fn().mockResolvedValue(undefined),
            commit: vi.fn().mockResolvedValue(undefined),
            abort: vi.fn().mockResolvedValue(undefined),
          }),
        }),
        admin: vi.fn().mockReturnValue({
          connect: vi.fn().mockResolvedValue(undefined),
          disconnect: vi.fn().mockResolvedValue(undefined),
          describeCluster: vi.fn().mockResolvedValue({ controller: 1 }),
        }),
        consumer: vi.fn().mockReturnValue({
          connect: vi.fn().mockResolvedValue(undefined),
          disconnect: vi.fn().mockResolvedValue(undefined),
          subscribe: vi.fn().mockResolvedValue(undefined),
          run: vi.fn().mockResolvedValue(undefined),
          on: vi.fn(),
        }),
      };
    }),
  };
});

describe('KafkaClient Smoke Test', () => {
  const topics = createTopicConfig({
    USER_EVENTS: {
      name: 'user.events',
      schema: z.object({
        userId: z.string(),
        action: z.string(),
      }),
    },
    ORDER_EVENTS: {
      name: 'order.events',
      schema: z.object({
        orderId: z.string(),
        amount: z.number(),
      }),
    },
  });

  let client: KafkaClient;

  beforeEach(() => {
    vi.clearAllMocks();
    client = new KafkaClient('smoke-test-client', ['localhost:9092'], {
      topics,
      defaultMetadata: { service: 'smoke-test', version: '1.0.0' },
      defaultHeaders: { 'x-service': 'smoke-test' },
    });
  });

  describe('Initialization', () => {
    it('should create client with all configurations', () => {
      expect(client).toBeDefined();
      expect(client.topics).toBeDefined();
      expect(client.topics?.USER_EVENTS).toBeDefined();
      expect(client.getDefaultMetadata()).toEqual({ service: 'smoke-test', version: '1.0.0' });
      expect(client.getDefaultHeaders()).toEqual({ 'x-service': 'smoke-test' });
    });

    it('should validate clientId', () => {
      expect(() => new KafkaClient('', ['localhost:9092'])).toThrow('clientId is required');
    });
  });

  describe('Connection Management', () => {
    it('should connect successfully', async () => {
      await client.connect();
      expect(client.isReady()).toBe(true);
    });

    it('should disconnect successfully', async () => {
      await client.connect();
      await client.disconnect();
      expect(client.isReady()).toBe(false);
    });

    it('should perform health check', async () => {
      const isHealthy = await client.healthCheck();
      expect(isHealthy).toBe(true);
    });
  });

  describe('Message Production', () => {
    beforeEach(async () => {
      await client.connect();
    });

    it('should send single message', async () => {
      await client.sendMessage('test-topic', {
        key: 'test-key',
        value: { data: 'test' },
      });

      const mockProducer = (client as any).producer;
      expect(mockProducer.send).toHaveBeenCalled();
    });

    it('should send batch messages', async () => {
      await client.sendMessages('test-topic', [
        { key: 'key1', value: { data: 'test1' } },
        { key: 'key2', value: { data: 'test2' } },
      ]);

      const mockProducer = (client as any).producer;
      expect(mockProducer.send).toHaveBeenCalled();
    });

    it('should merge default metadata and headers', async () => {
      await client.sendMessages('test-topic', [
        { 
          key: 'key1', 
          value: { data: 'test' },
          metadata: { requestId: '123' },
          headers: { 'x-request-id': '123' }
        }
      ]);

      const mockProducer = (client as any).producer;
      const call = mockProducer.send.mock.calls[0][0];
      const sentMessage = call.messages[0];

      expect(sentMessage.headers).toEqual({
        'x-service': 'smoke-test',
        'x-request-id': '123'
      });
    });

    it('should validate topic is not empty', async () => {
      await expect(
        client.sendMessages('', [{ key: 'key1', value: { data: 'test' } }])
      ).rejects.toThrow('topic is required');
    });

    it('should validate messages array is not empty', async () => {
      await expect(
        client.sendMessages('test-topic', [])
      ).rejects.toThrow('messages array cannot be empty');
    });

    it('should validate individual messages have value', async () => {
      await expect(
        client.sendMessages('test-topic', [
          { key: 'key1', value: null as any }
        ])
      ).rejects.toThrow('Message at index 0 is missing value');
    });
  });

  describe('Transactions', () => {
    beforeEach(async () => {
      await client.connect();
    });

    it('should execute transaction successfully', async () => {
      const result = await client.runTransaction(async (txn) => {
        await txn.send({
          topic: 'test-topic',
          messages: [{ key: 'key1', value: 'test' }]
        });
        return 'success';
      });

      expect(result).toBe('success');
    });

    it('should abort transaction on error', async () => {
      await expect(
        client.runTransaction(async () => {
          throw new Error('Transaction failed');
        })
      ).rejects.toThrow('Kafka transaction failed');
    });
  });

  describe('Consumer Management', () => {
    beforeEach(async () => {
      await client.connect();
    });

    it('should create consumer with valid groupId', async () => {
      const consumer = await client.createConsumer('test-group', {
        groupId: 'test-group'
      });

      expect(consumer).toBeDefined();
    });

    it('should validate groupId is not empty', async () => {
      await expect(
        client.createConsumer('', { groupId: '' })
      ).rejects.toThrow('groupId is required');
    });
  });

  describe('Topics Integration', () => {
    it('should access topics via client.topics', () => {
      expect(client.topics?.USER_EVENTS.name).toBe('user.events');
      expect(client.topics?.ORDER_EVENTS.name).toBe('order.events');
    });

    it('should provide type-safe topic access', () => {
      const userTopic = client.topics?.USER_EVENTS;
      expect(userTopic?.schema).toBeDefined();
    });
  });

  describe('Metrics', () => {
    it('should track metrics', async () => {
      await client.connect();
      await client.sendMessage('test-topic', {
        key: 'key1',
        value: { data: 'test' }
      });

      const metrics = client.getMetrics();
      expect(metrics).toBeDefined();
      expect(metrics.publishSuccess).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Admin Operations', () => {
    it('should provide admin client', () => {
      const admin = client.getAdmin();
      expect(admin).toBeDefined();
    });
  });

  describe('Producer Factory', () => {
    it('should create additional producers', () => {
      const producer = client.createProducer({
        idempotent: true
      });
      expect(producer).toBeDefined();
    });

    it('should get main producer', () => {
      const producer = client.getProducer();
      expect(producer).toBeDefined();
    });
  });
});
