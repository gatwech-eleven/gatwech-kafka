import { describe, it, expect, vi } from 'vitest';
import { BaseKafkaClient } from '../../../src/client/core/kafka';

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
      };
    }),
  };
});

describe('Batch Send Validation', () => {
  const client = new BaseKafkaClient('test-client', ['localhost:9092']);

  it('should throw error if any message in batch is missing value', async () => {
    (client as any).isConnected = true;
    const messages = [
      { value: 'valid', key: '1' },
      { value: undefined as any, key: '2' }, // Invalid
    ];
    await expect(client.sendMessages('test-topic', messages))
      .rejects
      .toThrow('Message at index 1 is missing value');
  });
});
