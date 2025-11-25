# Consumer Example for Express Demo App

This guide demonstrates how to create and use Kafka consumers in the express-demo app using the `@gatwech/kafka` package.

## Overview

The consumer example shows:
- **Type-safe message consumption** using Zod schemas
- **Dead Letter Queue (DLQ)** handling for failed messages
- **Automatic retry logic** with exponential backoff
- **Integration with Express app lifecycle** (startup and graceful shutdown)
- **Core API usage** - `consume()` is a built-in method on the Kafka client

## Files

### `src/consumers.ts`

Contains consumer setup and management:
- `initializeConsumers()` - Starts all consumers
- `shutdownConsumers()` - Gracefully stops all consumers

### `src/client.ts`

Creates the Kafka client with all core functionality enabled by default:
```typescript
export const kafka = createKafkaClient({
  config: {
    clientId: SERVICE_NAME,
    brokers: KAFKA_BROKERS,
    // ... other config
  },
});

// consume() is available directly on the client
```

### `src/index.ts`

Integrates consumers into the app lifecycle:
- Consumers start after successful Kafka initialization
- Consumers shutdown before Kafka disconnect

## Usage

### Creating a Consumer

```typescript
// Basic usage with type-safe payload
const consumer = await kafka.consume(
  'my-consumer-group',
  AppTopics.USER_EVENTS,
  async (payload, ctx) => {
    // payload is automatically typed as:
    // { userId: string, event: string, timestamp: number }
    console.log('User:', payload.userId);
    console.log('Event:', payload.event);
    
    // ctx provides topic config and groupId
    console.log('Topic:', ctx.topic.name);
    console.log('Group:', ctx.groupId);
  },
  {
    // DLQ options
    dlqEnabled: true,
    maxRetries: 3,
    retryBackoffMs: 1000,
  }
);
```

### DLQ Configuration

The DLQ options control retry and error handling:

- **`dlqEnabled`**: Enable/disable dead letter queue (default: `true`)
- **`maxRetries`**: Number of retry attempts (default: `3`)
- **`retryBackoffMs`**: Initial backoff delay in milliseconds (default: `500`)
- **`dlqSuffix`**: Suffix for DLQ topic name (default: `".dlq"`)

When a message fails after all retries, it's automatically sent to a DLQ topic named `<original-topic><dlqSuffix>`.

### Type Safety

The `consume` method provides two overloads:

1. **With Schema** - Payload is typed based on the schema:
```typescript
// AppTopics.USER_EVENTS has a schema
await kafka.consume(
  'group-id',
  AppTopics.USER_EVENTS,
  async (payload) => {
    // payload: { userId: string, event: string, timestamp: number }
  }
);
```

2. **Without Schema** - Raw KafkaJS payload:
```typescript
// Topic with no schema
await kafka.consume(
  'group-id',
  rawTopic,
  async (payload) => {
    // payload: EachMessagePayload from kafkajs
    const value = payload.message.value?.toString();
  }
);
```

## Running the Example

1. Start Kafka:
```bash
cd /home/jeremy/Projects/payvo-canary/packages/kafka
docker-compose up -d
```

2. Run the app:
```bash
cd examples/express-app
npm run dev
```

3. Send a test message via the API:
```bash
curl -X POST http://localhost:3000/events/user \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123", "event": "login"}'
```

4. Check the logs to see the consumer processing the message.

## Testing DLQ Behavior

To test DLQ functionality, publish an event with type `"simulate-error"`:

```bash
curl -X POST http://localhost:3000/events/user \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-456", "event": "simulate-error"}'
```

The consumer will:
1. Attempt to process the message
2. Fail and retry 3 times with exponential backoff
3. Send the message to the DLQ topic after all retries fail

## Consumer Groups

Each consumer belongs to a consumer group. Messages are load-balanced across consumers in the same group:
- `express-demo-user-events-group` - Processes user events
- `express-demo-order-events-group` - Processes order events

## Best Practices

1. **Use descriptive group IDs** - Include service name and topic for clarity
2. **Configure appropriate retry counts** - Balance between resilience and performance
3. **Monitor DLQ topics** - Set up alerts for messages in DLQ
4. **Handle idempotency** - Consumers may receive the same message multiple times
5. **Graceful shutdown** - Always disconnect consumers before Kafka client

## TypeScript Note

You may see TypeScript warnings about `payload` being of type `unknown`. This is a limitation of TypeScript's type inference through generic function overloads. The runtime behavior is correct, and the payload will be properly typed. This is a known limitation and does not affect functionality.

To work around this, you can explicitly type the payload:

```typescript
await kafka.consume(
  'group-id',
  AppTopics.USER_EVENTS,
  async (payload: { userId: string; event: string; timestamp: number }, ctx) => {
    // Now TypeScript knows the payload type
  }
);
```

## Further Reading

- [KafkaJS Documentation](https://kafka.js.org/)
- [Consumer Groups](https://kafka.apache.org/documentation/#consumergroup)
- [Dead Letter Queue Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html)
