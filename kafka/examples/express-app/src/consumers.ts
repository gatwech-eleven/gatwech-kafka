import { kafka, customLogger } from './client';
import { AppTopics } from './topics';
import type { Consumer } from '@gatwech/kafka';

let userEventsConsumer: Consumer | null = null;
let orderEventsConsumer: Consumer | null = null;

/**
 * Initialize all consumers for the application.
 * This demonstrates how to create consumers with type-safe message handling.
 */
export async function initializeConsumers() {
  try {
    customLogger.info('Initializing Kafka consumers...');

    // Consumer 1: User Events Consumer
    // Demonstrates type-safe consumption with schema validation
    userEventsConsumer = await kafka.consume(
      'express-demo-user-events-group',
      AppTopics.USER_EVENTS,
      async (payload, ctx) => {
        // payload is automatically typed as { userId: string, event: string, timestamp: number }
        customLogger.info('Processing user event', {
          userId: payload.userId,
          event: payload.event,
          timestamp: new Date(payload.timestamp).toISOString(),
          topic: ctx.topic.name,
          groupId: ctx.groupId,
        });

        // Simulate processing
        // Example: Store user activity, trigger notifications, etc.
        
        // Simulate occasional errors for DLQ demonstration
        if (payload.event === 'simulate-error') {
          throw new Error('Simulated error for DLQ demonstration');
        }

        customLogger.info('User event processed successfully', {
          userId: payload.userId,
          event: payload.event,
        });
      },
      {
        // DLQ configuration
        dlqEnabled: true,
        maxRetries: 3,
        retryBackoffMs: 1000,
      }
    );

    customLogger.info('User events consumer started successfully');

    // Consumer 2: Order Events Consumer
    // Demonstrates another consumer with different business logic
    orderEventsConsumer = await kafka.consume(
      'express-demo-order-events-group',
      AppTopics.ORDER_EVENTS,
      async (payload, ctx) => {
        // payload is automatically typed based on ORDER_EVENTS schema
        customLogger.info('Processing order event', {
          orderId: payload.orderId,
          userId: payload.userId,
          amount: payload.amount,
          status: payload.status,
          timestamp: new Date(payload.timestamp).toISOString(),
          topic: ctx.topic.name,
          groupId: ctx.groupId,
        });

        // Business logic example
        if (payload.status === 'completed') {
          customLogger.info('Order completed, triggering fulfillment', {
            orderId: payload.orderId,
            amount: payload.amount,
          });
          // Example: Trigger warehouse system, send confirmation email, etc.
        } else if (payload.status === 'cancelled') {
          customLogger.info('Order cancelled, processing refund', {
            orderId: payload.orderId,
          });
          // Example: Process refund, restore inventory, etc.
        }

        customLogger.info('Order event processed successfully', {
          orderId: payload.orderId,
          status: payload.status,
        });
      },
      {
        // DLQ configuration
        dlqEnabled: true,
        maxRetries: 5,
        retryBackoffMs: 2000,
      }
    );

    customLogger.info('Order events consumer started successfully');
    customLogger.info('All consumers initialized successfully');

    return true;
  } catch (error) {
    customLogger.error('Failed to initialize consumers', { error });
    throw error;
  }
}

/**
 * Gracefully shutdown all consumers.
 * This should be called during application shutdown.
 */
export async function shutdownConsumers() {
  try {
    customLogger.info('Shutting down consumers...');

    if (userEventsConsumer) {
      await userEventsConsumer.disconnect();
      customLogger.info('User events consumer disconnected');
    }

    if (orderEventsConsumer) {
      await orderEventsConsumer.disconnect();
      customLogger.info('Order events consumer disconnected');
    }

    customLogger.info('All consumers shut down successfully');
  } catch (error) {
    customLogger.error('Error shutting down consumers', { error });
    throw error;
  }
}

