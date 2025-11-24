import {
  createKafkaClient,
  type Logger,
  type KafkaClientConfig,
} from '@gatwech/kafka';
import { AppTopics } from './topics';

export const customLogger: Logger = {
  info: (msg: string, meta?: any) => console.log(`[INFO] ${new Date().toISOString()} - ${msg}`, meta || ''),
  warn: (msg: string, meta?: any) => console.warn(`[WARN] ${new Date().toISOString()} - ${msg}`, meta || ''),
  error: (msg: string, meta?: any) => console.error(`[ERROR] ${new Date().toISOString()} - ${msg}`, meta || ''),
};

const KAFKA_BROKERS = process.env.KAFKA_BROKERS?.split(',') || ['localhost:9092'];
const SERVICE_NAME = process.env.SERVICE_NAME || 'express-demo-service';

export const kafka = createKafkaClient({
  config: {
    clientId: SERVICE_NAME,
    brokers: KAFKA_BROKERS,
    allowAutoTopicCreation: false,
    metricsEnabled: true,
    topics: AppTopics,
    topicProvisioning: {
      partitions: 3,
      replicationFactor: 1,
    },
    resilience: {
      enabled: true,
      retry: {
        maxAttempts: 5,
        initialDelayMs: 1000,
        maxDelayMs: 30000,
        backoffMultiplier: 2,
      },
      /**
       * Circuit Breaker with Half-Open State
       * Opens after 5 consecutive failures, Waits 30 seconds before allowing a test request (Half-Open)
       * If test succeeds, circuit closes; if fails, reopens
       */
      circuitBreaker: {
        enabled: true,
        failureThreshold: 5,
        resetTimeoutMs: 30000, // 30 seconds Half-Open timeout
      },
      /**
       * Background Reconnection Monitor. Checks connection every 60 seconds,
       * prevents overlapping reconnection attempts (concurrency-safe),
       * re-provisions topics after successful reconnection
       */
      reconnection: {
        enabled: true,
        intervalMs: 60000, // Check every 60 seconds
        onReconnect: async () => {
          customLogger.info('Reconnected to Kafka! Re-provisioning topics...');
          try {
            await kafka.provisionTopics();
            customLogger.info('Topics re-provisioned successfully');
          } catch (error) {
            customLogger.error('Failed to re-provision topics after reconnection', error);
          }
        },
      },
    },
  },
} satisfies KafkaClientConfig);

export async function initKafka() {
  try {
    customLogger.info('Initializing Kafka connection...');
    
    await kafka.connect();
    customLogger.info('Connected to Kafka successfully');
    
    customLogger.info('Provisioning topics...');
    await kafka.provisionTopics();
    customLogger.info('Topics provisioned successfully');
    
    return true;
  } catch (error) {
    customLogger.error('Kafka initialization failed', error);
    customLogger.info('Background reconnection will retry automatically...');
    return false;
  }
}

export async function shutdownKafka() {
  try {
    await kafka.disconnect();
    customLogger.info('Kafka disconnected');
  } catch (error) {
    customLogger.error('Error disconnecting Kafka', error);
    throw error;
  }
}
