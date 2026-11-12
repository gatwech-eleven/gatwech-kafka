# Kafka Client Resilience & Error Handling

This document explains the built-in resilience features in `@gatwech/kafka` v0.0.9-beta.2 and how to configure them for production use.

## Overview

The Kafka client includes three layers of resilience:

1. **Retry Logic** - Automatic retries with exponential backoff
2. **Circuit Breaker** - Prevents cascading failures
3. **Reconnection Monitor** - Background reconnection attempts

These features work together to ensure your application continues running even when Kafka is temporarily unavailable.

---

## Quick Start

```typescript
import { createKafkaClient } from '@gatwech/kafka';

const client = createKafkaClient({
  config: {
    clientId: 'my-service',
    brokers: ['localhost:9092'],
    
    // Enable resilience features
    resilience: {
      enabled: true,
      circuitBreaker: {
        enabled: true,
        failureThreshold: 5,
        resetTimeoutMs: 30000,
      },
      reconnection: {
        enabled: true,
        intervalMs: 60000,
        onReconnect: async () => {
          console.log('Reconnected to Kafka!');
        },
      },
    },
  },
});
```

---

## Resilience Features

### 1. Retry Logic with Exponential Backoff

When a connection attempt fails, the client automatically retries with increasing delays.

**Default Behavior:**
- **Attempt 1**: Immediate
- **Attempt 2**: 2 seconds delay
- **Attempt 3**: 4 seconds delay
- **Attempt 4**: 8 seconds delay
- **Attempt 5**: 16 seconds delay

**Example Logs:**
```
2025-11-25T16:36:08.339Z [kafka] warn: connectWithRetry: connect failed, retrying 
  {"attempt":2,"delay":4000,"retriable":false,"error":"Connection error: connect ECONNREFUSED"}
```

**Configuration:**

The retry behavior is built-in and uses the client's `maxConnectionRetries` and `retryDelayMs` options:

```typescript
config: {
  maxConnectionRetries: 5,  // Default: 5 attempts
  retryDelayMs: 1000,       // Default: 1 second base delay
}
```

---

### 2. Circuit Breaker

Prevents your application from repeatedly attempting to connect to a failing Kafka broker.

**States:**
- **Closed** (Normal): All requests go through
- **Open** (Failed): Requests are blocked, error returned immediately
- **Half-Open** (Testing): After timeout, allows one test request

**How it Works:**

1. After `failureThreshold` consecutive failures, circuit opens
2. Circuit stays open for `resetTimeoutMs` milliseconds
3. After timeout, circuit enters half-open state
4. If next request succeeds, circuit closes; if fails, reopens

**Configuration:**

```typescript
circuitBreaker: {
  enabled: true,
  failureThreshold: 5,      // Open after 5 failures
  resetTimeoutMs: 30000,    // Wait 30s before retry
}
```

**Example:**

```typescript
// After 5 failed connection attempts:
await client.connect();
// Error: Circuit breaker is open - too many failed connection attempts

// After 30 seconds, circuit allows retry
await client.connect();  // Attempts connection again
```

**Manual Reset:**

```typescript
client.resetCircuitBreaker();  // Force circuit to close
```

---

### 3. Reconnection Monitor

Runs in the background, periodically checking and attempting to restore connection.

**How it Works:**

1. Runs every `intervalMs` milliseconds
2. Checks if client is disconnected
3. Attempts reconnection if needed
4. Calls `onReconnect` callback on success
5. Prevents overlapping reconnection attempts (concurrency-safe)

**Configuration:**

```typescript
reconnection: {
  enabled: true,
  intervalMs: 60000,  // Check every 60 seconds
  onReconnect: async () => {
    console.log('Reconnected to Kafka!');
    
    // Re-provision topics after reconnection
    await client.provisionTopics();
  },
}
```

**Important Notes:**

- `onReconnect` is **only called on successful reconnection**
- Reconnection attempts respect the circuit breaker
- Monitor stops when `client.disconnect()` is called

---

## Graceful Degradation Pattern

Your application should start and continue running even when Kafka is unavailable.

### Recommended Setup

```typescript
// client.ts
export const kafka = createKafkaClient({
  config: {
    clientId: 'payment-service',
    brokers: ['localhost:9092'],
    resilience: {
      enabled: true,
      circuitBreaker: {
        enabled: true,
        failureThreshold: 5,
        resetTimeoutMs: 30000,
      },
      reconnection: {
        enabled: true,
        intervalMs: 60000,
        onReconnect: async () => {
          logger.info('Kafka reconnected - re-provisioning topics');
          await kafka.provisionTopics();
        },
      },
    },
  },
});

export async function initializeKafkaClient() {
  try {
    logger.info('Connecting to Kafka...');
    await kafka.connect();
    
    logger.info('Provisioning topics...');
    await kafka.provisionTopics();
    
    logger.info('Kafka initialized successfully');
    return true;
  } catch (error) {
    logger.warn('Kafka unavailable - running in degraded mode');
    logger.debug('Error:', error);
    return false;  // ✅ Don't throw - let app continue
  }
}
```

### Server Startup

```typescript
// server.ts
async function start() {
  // Initialize services with graceful degradation
  const results = await Promise.allSettled([
    initializeKafkaClient(),
    initializeRedis(),
  ]);

  // Log results but continue regardless
  results.forEach((result, index) => {
    const service = index === 0 ? 'Kafka' : 'Redis';
    if (result.status === 'fulfilled') {
      logger.info(`${service}: initialized`);
    } else {
      logger.warn(`${service}: failed to initialize`);
    }
  });

  // ✅ Start server even if Kafka failed
  server.listen(PORT, () => {
    logger.info(`🚀 Server listening on port ${PORT}`);
    
    // Log Kafka status
    const kafkaStatus = kafka.isReady() ? 'connected' : 'reconnecting';
    logger.info(`Kafka status: ${kafkaStatus}`);
  });
}
```

---

## Health Monitoring

The client provides several methods to check connection status.

### Available Methods

```typescript
// Simple ready check
kafka.isReady()  // boolean

// Detailed connection status
kafka.getConnectionStatus()
// Returns:
// {
//   connected: boolean,
//   connectionAttempts: number,
//   lastError: string | null,
//   lastConnectedAt: Date | null,
//   lastDisconnectedAt: Date | null,
//   circuitBreakerOpen: boolean
// }

// Health status (recommended for health endpoints)
kafka.getHealthStatus()
// Returns:
// {
//   status: "healthy" | "degraded" | "unhealthy",
//   connected: boolean,
//   details: {
//     connectionAttempts: number,
//     lastError: string | null,
//     uptime: number | null  // milliseconds
//   }
// }

// Metrics
kafka.getMetrics()
// Returns:
// {
//   publishSuccess: number,
//   publishFailures: number,
//   connectionRetries: number,
//   consumerConnections: number
// }

// Deep health check (async - actually connects to broker)
await kafka.healthCheck()  // boolean
```

### Express Health Endpoints

```typescript
import { kafka } from './kafka/client';

// Simple health check
app.get('/health', (req, res) => {
  const health = kafka.getHealthStatus();
  const statusCode = health.status === 'healthy' ? 200 : 503;
  
  res.status(statusCode).json({
    status: health.status,
    kafka: health,
    timestamp: new Date().toISOString(),
  });
});

// Kubernetes readiness probe
app.get('/ready', (req, res) => {
  const ready = kafka.isReady();
  res.status(ready ? 200 : 503).json({ ready });
});

// Kubernetes liveness probe
app.get('/live', (req, res) => {
  const health = kafka.getHealthStatus();
  const alive = health.status !== 'unhealthy';
  res.status(alive ? 200 : 503).json({ alive });
});
```

---

## Expected Behavior

### When Kafka is Down

**What You'll See:**

```
{"level":"ERROR","logger":"kafkajs","message":"[Connection] Connection error: connect ECONNREFUSED"}
2025-11-25T16:36:08.339Z [kafka] warn: connectWithRetry: connect failed, retrying 
  {"attempt":2,"delay":4000}
...
2025-11-25T16:37:00.026Z [kafka] error: connectWithRetry: no more retries, failing
[2025-11-25T16:37:00.030Z] [Service] ERROR: Kafka initialization failed
[2025-11-25T16:37:00.030Z] [Service] INFO: Background reconnection will retry automatically
[2025-11-25T16:37:00.035Z] [Service] INFO: 🚀 Server listening at http://0.0.0.0:5002
```

**What's Happening:**

1. ✅ Client attempts connection with exponential backoff
2. ✅ After 5 retries, initial connection fails
3. ✅ App continues and starts server anyway
4. ✅ Reconnection monitor will retry every 60 seconds
5. ✅ Circuit breaker prevents cascading failures
6. ✅ Health endpoints return degraded status

**This is CORRECT behavior** - your app is resilient!

### When Kafka Comes Back

**What You'll See:**

```
2025-11-25T16:38:00.123Z [kafka] info: Reconnection attempt successful
2025-11-25T16:38:00.124Z [Service] INFO: Kafka reconnected - re-provisioning topics
2025-11-25T16:38:00.456Z [Service] INFO: Topics re-provisioned successfully
```

**What's Happening:**

1. ✅ Reconnection monitor detects Kafka is back
2. ✅ Connection is restored automatically
3. ✅ `onReconnect` callback is triggered
4. ✅ Topics are re-provisioned
5. ✅ App is fully operational again

---

## Production Best Practices

### 1. Configure Appropriate Timeouts

```typescript
resilience: {
  enabled: true,
  circuitBreaker: {
    enabled: true,
    failureThreshold: 3,      // Open circuit faster in prod
    resetTimeoutMs: 60000,    // Wait longer before retry
  },
  reconnection: {
    enabled: true,
    intervalMs: 30000,        // Check more frequently
  },
}
```

### 2. Add Structured Logging

```typescript
reconnection: {
  enabled: true,
  intervalMs: 60000,
  onReconnect: async () => {
    logger.info('Kafka reconnected', {
      uptime: kafka.getHealthStatus().details.uptime,
      metrics: kafka.getMetrics(),
    });
    
    try {
      await kafka.provisionTopics();
      logger.info('Topics re-provisioned successfully');
    } catch (error) {
      logger.error('Topic provisioning failed', { error });
    }
  },
}
```

### 3. Monitor Circuit Breaker State

```typescript
// Expose circuit breaker state in metrics
app.get('/metrics', (req, res) => {
  const status = kafka.getConnectionStatus();
  
  res.json({
    kafka_circuit_breaker_open: status.circuitBreakerOpen ? 1 : 0,
    kafka_connection_attempts: status.connectionAttempts,
    kafka_connected: status.connected ? 1 : 0,
  });
});
```

### 4. Implement Graceful Shutdown

```typescript
async function handleShutdown(signal: string) {
  logger.info(`Received ${signal}, shutting down gracefully`);
  
  // Close HTTP server first
  server.close();
  
  // Disconnect Kafka
  try {
    await kafka.disconnect();
    logger.info('Kafka disconnected');
  } catch (error) {
    logger.error('Error disconnecting Kafka', error);
  }
  
  process.exit(0);
}

process.on('SIGTERM', () => handleShutdown('SIGTERM'));
process.on('SIGINT', () => handleShutdown('SIGINT'));
```

---

## Troubleshooting

### Circuit Breaker Stuck Open

**Symptom:** Circuit breaker won't close even though Kafka is back up.

**Solution:**

```typescript
// Manually reset circuit breaker
kafka.resetCircuitBreaker();

// Or wait for resetTimeoutMs to elapse
```

### Too Many Retry Logs

**Symptom:** Logs are flooded with retry attempts.

**Solution:**

Reduce retry attempts or increase initial delay:

```typescript
config: {
  maxConnectionRetries: 3,  // Fewer retries
  retryDelayMs: 2000,       // Longer initial delay
}
```

### App Won't Start Without Kafka

**Symptom:** App crashes if Kafka is unavailable.

**Solution:**

Don't throw errors in `initializeKafkaClient`:

```typescript
export async function initializeKafkaClient() {
  try {
    await kafka.connect();
    return true;
  } catch (error) {
    logger.warn('Kafka unavailable - degraded mode');
    return false;  // ✅ Return false, don't throw
  }
}
```

---

## Migration from v0.0.8

If you're upgrading from an earlier version:

### Old Pattern (v0.0.8)

```typescript
// Manual retry logic
let retries = 0;
while (retries < 5) {
  try {
    await kafka.connect();
    break;
  } catch (error) {
    retries++;
    await sleep(retries * 1000);
  }
}
```

### New Pattern (v0.0.9-beta.2)

```typescript
// Built-in resilience
const kafka = createKafkaClient({
  config: {
    // ... config
    resilience: { enabled: true },
  },
});

await kafka.connect();  // Automatic retries + circuit breaker
```

---

## Summary

The resilience features in `@gatwech/kafka` v0.0.9-beta.2 provide:

✅ **Automatic retries** with exponential backoff  
✅ **Circuit breaker** to prevent cascading failures  
✅ **Background reconnection** for automatic recovery  
✅ **Graceful degradation** - app continues when Kafka is down  
✅ **Health monitoring** - detailed status and metrics  
✅ **Production-ready** - battle-tested patterns

Your application will survive Kafka outages and automatically recover when service is restored!
