import { describe, it, expect, vi } from 'vitest';
import { KafkaClient } from '../../../src/client/core/kafka';

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
          send: vi.fn(),
        }),
        admin: vi.fn().mockReturnValue({
          connect: vi.fn(),
          disconnect: vi.fn(),
        }),
        consumer: vi.fn().mockReturnValue({
          connect: vi.fn(),
          disconnect: vi.fn(),
          on: vi.fn(),
        }),
      };
    }),
  };
});

describe('Producer & Consumer Validation', () => {
  const client = new KafkaClient('test-client', ['localhost:9092']);

  describe('Consumer Validation', () => {
    it('should throw error when groupId is empty', async () => {
      await expect(client.createConsumer('', { groupId: '' }))
        .rejects
        .toThrow('groupId is required');
    });
  });

  describe('Producer Validation', () => {
    it('should throw error when topic is empty', async () => {
      // Mock connection state
      (client as any).isConnected = true;
      await expect(client.sendMessages('', [{ value: 'test', key: 'key' }]))
        .rejects
        .toThrow('topic is required');
    });

    it('should throw error when messages array is empty', async () => {
      (client as any).isConnected = true;
      await expect(client.sendMessages('test-topic', []))
        .rejects
        .toThrow('messages array cannot be empty');
    });
  });
});
