import { describe, it, expect, vi } from 'vitest';
import { BaseKafkaClient } from '../../../src/client/core/kafka';
import { Kafka } from 'kafkajs';

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
        describeCluster: vi.fn().mockResolvedValue({ controller: 1 }),
      }),
    }; }),
  };
});

describe('KafkaClient Configuration', () => {
  it('should pass SSL and SASL options to KafkaJS', () => {
    const sslOptions = { rejectUnauthorized: false };
    const saslOptions = { mechanism: 'plain', username: 'user', password: 'password' };

    new BaseKafkaClient('test-client', ['localhost:9092'], {
      ssl: sslOptions,
      sasl: saslOptions as any, // Cast because mechanism string vs enum
      connectionTimeout: 5000,
      authenticationTimeout: 2000,
      reauthenticationThreshold: 10000,
    });

    expect(Kafka).toHaveBeenCalledWith(expect.objectContaining({
      ssl: sslOptions,
      sasl: saslOptions,
      connectionTimeout: 5000,
      authenticationTimeout: 2000,
      reauthenticationThreshold: 10000,
    }));
  });
});
