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
        }),
        admin: vi.fn().mockReturnValue({
          connect: vi.fn(),
          disconnect: vi.fn(),
        }),
      };
    }),
  };
});

describe('KafkaClient Validation', () => {
  it('should throw an error if clientId is empty', () => {
    expect(() => {
      new BaseKafkaClient('', ['localhost:9092']);
    }).toThrow('clientId is required');
  });
});
