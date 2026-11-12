# @gatwech/kafka

Lightweight, production-grade Kafka client for Node.js with **strong typing**, **runtime validation**, and **built-in reliability patterns** using KafkaJS + Zod.

## Table of Contents

- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API Reference](#api-reference)
- [Architecture & Design](#architecture--design)
- [Best Practices](#best-practices)
- [Configuration](#configuration)
- [Testing](#testing)
- [Migration Guide](#migration-guide)
- [License](#license)

## Quick Start

### Producer (Service Factory Pattern - Recommended)

```ts
import { Kafka } from "kafkajs";
import { z } from "@gatwech/utils/zod";
import { createServicePublisherFactory } from "@gatwech/kafka";

const kafka = new Kafka({ clientId: "users", brokers: ["localhost:9092"] });
const producer = kafka.producer();
await producer.connect();

const service = createServicePublisherFactory(producer, "user-service-v1");

const UserRegistered = z.object({
  id: z.string(),
  email: z.string().email(),
  timestamp: z.number().optional(),
  metadata: z.object({
    correlationId: z.string().optional(),
    source: z.string().optional(),
  }).optional(),
});

const getPartitionKey = (p: z.infer<typeof UserRegistered>) => p.id;
const publishUserRegistered = service(
  "user.registered",
  UserRegistered,
  getPartitionKey
);

await publishUserRegistered({ id: "123", email: "alice@example.com" });
```

**Auto-injected metadata:** `timestamp` (epoch ms), `metadata.source`, `metadata.correlationId`  
**Headers:** `correlation-id`, `event-type`, `timestamp`, optionally `schema-hash`

### Consumer with Retry & DLQ

```ts
import { consumeWithRetryAndDLQ, createKafkaClient } from "@gatwech/kafka";

const client = createKafkaClient({
  config: {
    clientId: "svc",
    brokers: ["localhost:9092"],
  },
});
await client.connect();

const topicConfig = {
  name: "user.registered",
  schema: UserRegistered, // Optional: enables type-safe payload inference
};

await consumeWithRetryAndDLQ(
  client,
  "user-service-group",
  topicConfig,
  async (payload, ctx) => {
    // payload is fully typed and validated when schema is provided
    console.log(`Processing user: ${payload.email}`);
    // Throws trigger retry -> DLQ after max attempts
  },
  {
    maxRetries: 3,
    retryBackoffMs: 500,
    dlqEnabled: true,
  }
);
```

## Core Concepts

### Event Publishing (Producers)

**Service Factory** (`createServicePublisherFactory`): Best for services with multiple event types sharing metadata.

**Event Factory** (`createEventPublisher`): Low-level, for one-off publishers or custom scenarios.

**Type Safety**: All publishers validate payloads against Zod schemas before sending.

### Event Consumption (Consumers)

**Type-Safe Consumers**: When a topic config includes a `schema`, the consumer handler receives fully-typed, validated payloads.

**Retry Logic**: Failed messages are retried with exponential backoff.

**Dead Letter Queue (DLQ)**: Messages that fail after max retries are sent to `${topic}.dlq` with error metadata.

### Type Safety with Zod

Define schemas for all topics:

```ts
const OrderPlacedSchema = z.object({
  orderId: z.string(),
  items: z.array(z.object({
    productId: z.string(),
    quantity: z.number().positive(),
    price: z.number().positive(),
  })),
  total: z.number().positive(),
  timestamp: z.number(),
});
```

Benefits:
- **Compile-time** type checking with `z.infer<typeof Schema>`
- **Runtime** validation to catch malformed messages
- **Self-documenting** event contracts

### Dead Letter Queues (DLQ)

DLQ messages include:
- Original message value, key, headers
- Error message and stack trace
- Source topic, partition, offset, consumer group

Monitor DLQ depth as your primary signal for processing issues.

## API Reference

### Producers

#### `createServicePublisherFactory(producer, serviceSource)`

Creates a factory for defining multiple event publishers sharing the same producer and source metadata.

Returns: `(topic, schema, getPartitionKey) => EventPublisher<T>`

#### `createEventPublisher(producer, topic, schema, keyDeriver, source)`

Creates a single event publisher for a specific topic.

Returns: `(payload) => Promise<void>`

#### `createDLQPublisher(producer, opts?)`

Creates a reusable DLQ publisher.

```ts
const publishToDLQ = createDLQPublisher(producer, { dlqSuffix: ".dlq" });

await publishToDLQ(
  { value: Buffer.from("..."), key: Buffer.from("..."), headers: {} },
  new Error("validation failed"),
  { topic: "user.registered", partition: 0, offset: "105", groupId: "svc-a" }
);
```

### Consumers

#### `consumeWithRetryAndDLQ(client, groupId, topic, handler, opts?)`

Runs a consumer with automatic retry and DLQ handling.

**Options:**
- `maxRetries` (default: 3)
- `retryBackoffMs` (default: 500)
- `dlqEnabled` (default: true)
- `dlqSuffix` (default: ".dlq")

**Type Inference:** When `topic` includes a `schema`, the handler's `payload` param is automatically typed.

### Client Configuration

#### `createKafkaClient(options)`

Creates a modular Kafka client with extension support.

```ts
const client = createKafkaClient({
  config: {
    clientId: "my-service",
    brokers: ["localhost:9092"],
    connectWithRetries: true,      // Enable connection retries
    maxConnectAttemptTimeMs: 30000, // Timeout for broker readiness
    connectPollIntervalMs: 1000,    // Poll interval for readiness check
    // New: Standard KafkaJS options
    ssl: true,
    sasl: { mechanism: "plain", username: "user", password: "password" },
  },
  extensions: [
    topicProvisioningExtension(topics),
    // healthCheck is now built-in: client.healthCheck()
  ],
});
```

**Key Options:**
- `connectWithRetries`: Wait for broker readiness before connecting
- `connectAsync`: Non-blocking async connection
- `autoConnectAsync`: Auto-start async connection on instantiation
- `maxConnectionRetries`: Max producer connection retry attempts
- `retryDelayMs`: Delay between retries
- `metricsEnabled`: Enable publish success/failure metrics

### Utilities

#### `connectWithRetry(connectFn, options?)`

Helper for retrying async operations with exponential backoff.

```ts
await connectWithRetry(() => client.connect(), {
  maxAttempts: 10,
  initialDelayMs: 500,
});
```

## Architecture & Design

### Connection Management & Retries

**Problem**: Kafka may be unavailable at startup (DNS issues, broker restarts, network partitions).

**Solution**: The client implements bounded retries with exponential backoff:

1. **Broker Readiness Check**: When `connectWithRetries: true`, the client polls the cluster controller before connecting the producer
2. **Bounded Retries**: Configurable max attempts prevent infinite blocking
3. **Async Startup**: Use `connectAsync: true` to avoid blocking HTTP server startup
4. **Lazy Admin Client**: Admin client connects only when needed, avoiding startup delays

**Configuration:**
```ts
new KafkaClient("svc", brokers, {
  connectWithRetries: true,
  maxConnectionRetries: 10,
  retryDelayMs: 1000,
  maxConnectAttemptTimeMs: 30000,
  connectPollIntervalMs: 1000,
});
```

### Consumer Rebalances

**Optimizations**:
- Tuned `sessionTimeout`, `heartbeatInterval`, and `rebalanceTimeout` for faster recovery
- Automatic reconnection on consumer crashes with exponential backoff
- Event listeners for `consumer.connect`, `consumer.disconnect`, `consumer.group_join`

**Guaranteed Delivery**: Consumers only commit offsets **after** successful processing, ensuring at-least-once delivery.

### Message Guarantees & Idempotence

**Producer Guarantees**:
- **Idempotent producer** (`idempotent: true`): Prevents duplicate messages during retries
- **acks=all**: Message written to all in-sync replicas before success
- **Transactions**: `runTransaction()` for atomic multi-topic writes

**Consumer Guarantees**:
- **Manual commits**: Offsets committed only after successful processing
- **Idempotent design required**: Use upserts/INSERT ON CONFLICT in your handlers

**Result**: Effectively-once processing when combined with idempotent handlers.

### Schema Evolution Strategy

**Current Approach**: Zod schemas provide compile-time and runtime validation.

**Best Practices**:
- Add new fields as **optional** to maintain backward compatibility
- Never remove required fields (breaking change)
- Use schema versioning for major changes (e.g., `user.registered.v2`)
- Consider Schema Registry (Confluent, AWS Glue) for cross-team coordination

**Metadata Tracking**: Messages include `schema-hash` header for schema evolution tracking.

## Best Practices

### 1. Idempotent Consumers

Design handlers to be safely reprocessed:

```ts
// ✅ Good: Idempotent upsert
await db.user.upsert({
  where: { id: payload.id },
  create: { ...payload },
  update: { ...payload },
});

// ❌ Bad: Non-idempotent increment
await db.user.update({
  where: { id: payload.id },
  data: { loginCount: { increment: 1 } },
});
```

### 2. Error Handling

**Transient errors** (network issues, DB locks): Throw to trigger retry.

**Permanent errors** (invalid data): Log and return successfully to skip message (or implement custom DLQ logic).

```ts
async (payload, ctx) => {
  try {
    await processMessage(payload);
  } catch (err) {
    if (isTransient(err)) {
      throw err; // Retry
    }
    logger.error("Permanent error, skipping", { err, payload });
    // Don't throw = commit offset, message skipped
  }
}
```

### 3. Monitoring & Observability

**Key Metrics**:
- `publishSuccess` / `publishFailures`
- `connectionRetries`
- `consumerConnections`
- **Consumer lag** (most important): How far behind consumers are from topic end

**DLQ Monitoring**: Set alerts on DLQ topic depth (critical health indicator).

**Distributed Tracing**: Inject trace context into message headers for end-to-end visibility.

### 4. Performance Tuning

**Batching**: Use `sendBatch()` for high-throughput scenarios

**Partition Key Selection**: Choose keys that evenly distribute load (avoid hot partitions)

**Consumer Groups**: Scale out by adding consumers to the same group

**Compression**: Enable compression for large messages (`compression: CompressionTypes.GZIP`)

## Configuration

### Topic Provisioning

**Option 1: Script (Recommended for dev)**
```bash
cd packages/kafka
bash scripts/kafka-init.sh
```

**Option 2: Runtime (Opt-in)**
```bash
PROVISION_KAFKA_TOPICS=1 pnpm dev
```

**Option 3: CI/Docker**
```yaml
kafka-init:
  image: bash:5
  entrypoint: ["/bin/sh", "-c", "/workspace/packages/kafka/scripts/kafka-init.sh"]
  volumes:
    - ./:/workspace:cached
  depends_on:
    - kafka
  environment:
    BOOTSTRAP_SERVER: kafka:9092
```

### Environment Variables

- `KAFKA_BOOTSTRAP`: Broker addresses (default: `localhost:9092`)
- `PROVISION_KAFKA_TOPICS`: Enable runtime provisioning (`1` or `true`)
- Topic-specific overrides: `SERVICE_E2E_TOPIC`, `INTEGRATION_TOPIC`, etc.

### Client Options

See [DEFAULT_OPTIONS](./src/client/core/default-options.ts) for all available options and defaults.

## Testing

### E2E Tests

```bash
pnpm run test:e2e           # Basic producer/consumer roundtrip
pnpm run test:e2e:service   # Service publisher factory
pnpm run test:e2e:integration
pnpm run test:e2e:smoke     # Quick connectivity check
pnpm run test:e2e:express   # Express integration + DLQ
```

### Integration Testing Strategy

**Recommended**: Use Testcontainers to spin up real Kafka brokers in CI/CD:

```ts
import { GenericContainer } from "testcontainers";

const kafka = await new GenericContainer("confluentinc/cp-kafka:7.5.0")
  .withExposedPorts(9092)
  .start();

// Run tests against real broker
```

**Benefits**:
- Catches serialization, connection, and rebalance issues
- High confidence before production
- No mocks needed for critical paths

## Migration Guide

### Deprecations

#### Type Aliases
- `KeyDeriver<T>` → use `PartitionKeyFn<T>`

#### Function Names
- `createServicePublisherFactory` → Prefer `createServicePublisherFactory` with explicit `getPartitionKey` naming

#### Client Options
- `waitForBrokerBeforeConnect` → use `connectWithRetries` or `connectAsync`
- `brokerWaitTimeoutMs` → use `maxConnectAttemptTimeMs`
- `brokerWaitPollIntervalMs` → use `connectPollIntervalMs`

#### Producer Methods
- `producer.validateAndSend()` → use `createServicePublisherFactory()` or `createEventPublisher()`
- `producer.validateAndSendMany()` → use factory publishers with `sendBatch()`

**Timeline**: Deprecated APIs will be removed in the next major release. Migrate to factory-based publishers for stronger typing and metadata injection.

### Breaking Changes (Recent)

**Directory restructure** (v1.x): Client files moved to subdirectories:
- Core: `client/core/` (kafka.ts, create-client.ts, errors.ts, etc.)
- Producers: `client/producers/` (factories, publishers)
- Consumers: `client/consumers/` (DLQ, retry logic)

**Test Reorganization**:
- E2E tests moved from `e2e/` to `__tests__/e2e/`.

**Renaming**:
- `partition-calc` extension renamed to `partition-analysis`.

Public exports unchanged; internal imports require updates if importing directly from file paths.

## License

MIT (see repository root for details)
