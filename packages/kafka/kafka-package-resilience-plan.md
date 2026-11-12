# Package-Level Kafka Resilience Implementation

## Goal

Move Kafka resilience features (retry logic, circuit breaker, background reconnection, health monitoring) from application level to the `@gatwech/kafka` package, making these features available to all applications using the package without custom implementation.

## Benefits

1. **DRY Principle**: Eliminate code duplication across microservices
2. **Consistency**: All services get the same resilience behavior
3. **Maintainability**: Update resilience logic in one place
4. **Simplicity**: Applications just configure, don't implement
5. **Best Practices**: Package enforces production-ready patterns

---

## Proposed Package API Design

### 1. Enhanced KafkaClient Constructor

```typescript
interface KafkaClientOptions {
  // Existing options
  allowAutoTopicCreation?: boolean;
  
  // NEW: Resilience options
  resilience?: {
    enabled?: boolean;                    // Enable resilience features (default: true)
    retry?: {
      maxAttempts?: number;               // Default: 5
      initialDelayMs?: number;            // Default: 1000
      maxDelayMs?: number;                // Default: 30000
      backoffMultiplier?: number;         // Default: 2
    };
    reconnection?: {
      enabled?: boolean;                  // Default: true
      intervalMs?: number;                // Default: 60000 (60s)
      onReconnect?: () => Promise<void>;  // Callback after successful reconnection
    };
    circuitBreaker?: {
      enabled?: boolean;                  // Default: true
      failureThreshold?: number;          // Default: 5 (same as retry maxAttempts)
    };
  };
  
  // NEW: Logging
  logger?: {
    info: (message: string, meta?: any) => void;
    warn: (message: string, meta?: any) => void;
    error: (message: string, meta?: any) => void;
  };
}
```

### 2. Connection Status API

```typescript
class KafkaClient {
  // NEW: Get connection status
  getConnectionStatus(): {
    connected: boolean;
    connectionAttempts: number;
    lastError: string | null;
    lastConnectedAt: Date | null;
    lastDisconnectedAt: Date | null;
    circuitBreakerOpen: boolean;
  }
  
  // NEW: Check if connected (simple boolean)
  isConnected(): boolean;
  
  // NEW: Manual reconnection trigger
  reconnect(): Promise<boolean>;
  
  // Existing methods
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  // ...
}
```

### 3. Health Check Helper

```typescript
class KafkaClient {
  // NEW: Health check helper for monitoring
  getHealthStatus(): {
    status: 'healthy' | 'degraded' | 'unhealthy';
    connected: boolean;
    details: {
      connectionAttempts: number;
      lastError: string | null;
      uptime: number | null;  // ms since last successful connection
    };
  }
}
```

---

## Implementation Details for @gatwech/kafka Package

### File Structure

```
@gatwech/kafka/
├── src/
│   ├── client/
│   │   ├── KafkaClient.ts           # Main client class
│   │   ├── ConnectionManager.ts     # NEW: Handles connection with retry
│   │   ├── CircuitBreaker.ts        # NEW: Circuit breaker implementation
│   │   └── ReconnectionMonitor.ts   # NEW: Background reconnection
│   ├── types/
│   │   └── index.ts                 # Export all types including new options
│   └── index.ts
```

### 1. ConnectionManager Class

**Purpose**: Encapsulate retry logic with exponential backoff

```typescript
// src/client/ConnectionManager.ts
export class ConnectionManager {
  private retryConfig: RetryConfig;
  private logger: Logger;
  private connectionAttempts = 0;
  private lastError: string | null = null;
  
  constructor(config: RetryConfig, logger: Logger) {
    this.retryConfig = config;
    this.logger = logger;
  }
  
  async connectWithRetry(
    connectFn: () => Promise<void>
  ): Promise<boolean> {
    for (let attempt = 1; attempt <= this.retryConfig.maxAttempts; attempt++) {
      try {
        this.logger.info(`Attempting to connect (${attempt}/${this.retryConfig.maxAttempts})...`);
        await connectFn();
        
        this.connectionAttempts = 0;
        this.lastError = null;
        this.logger.info("Connected successfully");
        
        return true;
      } catch (error) {
        this.connectionAttempts = attempt;
        this.lastError = error instanceof Error ? error.message : String(error);
        
        if (attempt < this.retryConfig.maxAttempts) {
          const delay = this.calculateBackoff(attempt);
          this.logger.info(`Waiting ${delay}ms before retry...`);
          await this.sleep(delay);
        }
      }
    }
    
    this.logger.error("Connection failed after all retries");
    return false;
  }
  
  private calculateBackoff(attempt: number): number {
    const delay = this.retryConfig.initialDelayMs * 
                  Math.pow(this.retryConfig.backoffMultiplier, attempt - 1);
    return Math.min(delay, this.retryConfig.maxDelayMs);
  }
  
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  
  getStatus() {
    return {
      attempts: this.connectionAttempts,
      lastError: this.lastError,
    };
  }
}
```

### 2. CircuitBreaker Class

**Purpose**: Prevent cascading failures

```typescript
// src/client/CircuitBreaker.ts
export class CircuitBreaker {
  private failureCount = 0;
  private threshold: number;
  private isOpen = false;
  
  constructor(threshold: number) {
    this.threshold = threshold;
  }
  
  recordFailure(): void {
    this.failureCount++;
    if (this.failureCount >= this.threshold) {
      this.isOpen = true;
    }
  }
  
  recordSuccess(): void {
    this.failureCount = 0;
    this.isOpen = false;
  }
  
  isCircuitOpen(): boolean {
    return this.isOpen;
  }
  
  reset(): void {
    this.failureCount = 0;
    this.isOpen = false;
  }
}
```

### 3. ReconnectionMonitor Class

**Purpose**: Background reconnection attempts

```typescript
// src/client/ReconnectionMonitor.ts
export class ReconnectionMonitor {
  private interval: NodeJS.Timeout | null = null;
  private intervalMs: number;
  private logger: Logger;
  private onReconnect?: () => Promise<void>;
  
  constructor(
    intervalMs: number,
    logger: Logger,
    onReconnect?: () => Promise<void>
  ) {
    this.intervalMs = intervalMs;
    this.logger = logger;
    this.onReconnect = onReconnect;
  }
  
  start(
    isConnectedFn: () => boolean,
    reconnectFn: () => Promise<boolean>
  ): void {
    if (this.interval) return;
    
    this.logger.info(`Starting reconnection monitor (every ${this.intervalMs}ms)`);
    
    this.interval = setInterval(async () => {
      if (!isConnectedFn()) {
        this.logger.info("Attempting background reconnection...");
        const success = await reconnectFn();
        
        if (success && this.onReconnect) {
          try {
            await this.onReconnect();
          } catch (error) {
            this.logger.error("Reconnection callback failed", error);
          }
        }
      }
    }, this.intervalMs);
  }
  
  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }
}
```

### 4. Enhanced KafkaClient

**Purpose**: Integrate all resilience features

```typescript
// src/client/KafkaClient.ts
export class KafkaClient {
  private connectionManager: ConnectionManager;
  private circuitBreaker: CircuitBreaker;
  private reconnectionMonitor: ReconnectionMonitor;
  private isConnectedFlag = false;
  private lastConnectedAt: Date | null = null;
  private lastDisconnectedAt: Date | null = null;
  
  constructor(
    clientId: string,
    brokers: string[],
    options?: KafkaClientOptions
  ) {
    // ... existing initialization
    
    const resilienceConfig = options?.resilience || {};
    const retryConfig = resilienceConfig.retry || {};
    const reconnectionConfig = resilienceConfig.reconnection || {};
    
    // Initialize resilience components
    this.connectionManager = new ConnectionManager(
      {
        maxAttempts: retryConfig.maxAttempts || 5,
        initialDelayMs: retryConfig.initialDelayMs || 1000,
        maxDelayMs: retryConfig.maxDelayMs || 30000,
        backoffMultiplier: retryConfig.backoffMultiplier || 2,
      },
      this.logger
    );
    
    this.circuitBreaker = new CircuitBreaker(
      resilienceConfig.circuitBreaker?.failureThreshold || 5
    );
    
    this.reconnectionMonitor = new ReconnectionMonitor(
      reconnectionConfig.intervalMs || 60000,
      this.logger,
      reconnectionConfig.onReconnect
    );
  }
  
  async connect(): Promise<void> {
    if (this.circuitBreaker.isCircuitOpen()) {
      throw new Error("Circuit breaker is open - too many failed connection attempts");
    }
    
    const success = await this.connectionManager.connectWithRetry(
      async () => {
        await this.kafka.producer().connect();
        await this.kafka.admin().connect();
      }
    );
    
    if (success) {
      this.isConnectedFlag = true;
      this.lastConnectedAt = new Date();
      this.circuitBreaker.recordSuccess();
      
      // Start background reconnection monitoring
      if (this.options?.resilience?.reconnection?.enabled !== false) {
        this.reconnectionMonitor.start(
          () => this.isConnectedFlag,
          () => this.reconnect()
        );
      }
    } else {
      this.circuitBreaker.recordFailure();
      throw new Error("Failed to connect after all retry attempts");
    }
  }
  
  async disconnect(): Promise<void> {
    this.reconnectionMonitor.stop();
    
    if (this.isConnectedFlag) {
      await this.kafka.producer().disconnect();
      await this.kafka.admin().disconnect();
      this.isConnectedFlag = false;
      this.lastDisconnectedAt = new Date();
    }
  }
  
  async reconnect(): Promise<boolean> {
    try {
      await this.connect();
      return true;
    } catch {
      return false;
    }
  }
  
  isConnected(): boolean {
    return this.isConnectedFlag;
  }
  
  getConnectionStatus() {
    const managerStatus = this.connectionManager.getStatus();
    
    return {
      connected: this.isConnectedFlag,
      connectionAttempts: managerStatus.attempts,
      lastError: managerStatus.lastError,
      lastConnectedAt: this.lastConnectedAt,
      lastDisconnectedAt: this.lastDisconnectedAt,
      circuitBreakerOpen: this.circuitBreaker.isCircuitOpen(),
    };
  }
  
  getHealthStatus() {
    const status = this.getConnectionStatus();
    const uptime = status.lastConnectedAt 
      ? Date.now() - status.lastConnectedAt.getTime()
      : null;
    
    let healthStatus: 'healthy' | 'degraded' | 'unhealthy';
    if (status.connected) {
      healthStatus = 'healthy';
    } else if (status.circuitBreakerOpen) {
      healthStatus = 'unhealthy';
    } else {
      healthStatus = 'degraded';
    }
    
    return {
      status: healthStatus,
      connected: status.connected,
      details: {
        connectionAttempts: status.connectionAttempts,
        lastError: status.lastError,
        uptime,
      },
    };
  }
  
  getProducer(): Producer {
    if (!this.isConnectedFlag) {
      throw new Error("Kafka is not connected. Call connect() first.");
    }
    return this.kafka.producer();
  }
}
```

---

## Simplified Application Code (After Package Update)

### Before (Current - 217 lines in kafka.ts)

```typescript
// Lots of custom retry logic, circuit breaker, reconnection code...
```

### After (With Package Features - ~30 lines)

```typescript
// src/infra/config/kafka.ts
import { config } from "@/infra/config/env";
import { logger } from "@/shared/logger";
import { ensureTopicsWithConfig, KafkaClient } from "@gatwech/kafka";

export const client = new KafkaClient(
  config.serviceName || "payment-service",
  config.kafka.brokers,
  {
    allowAutoTopicCreation: false,
    resilience: {
      enabled: config.kafka.enabled,
      retry: {
        maxAttempts: 5,
        initialDelayMs: 1000,
        maxDelayMs: 30000,
      },
      reconnection: {
        enabled: true,
        intervalMs: 60000,
        onReconnect: async () => {
          // Reinitialize topics after reconnection
          await ensureTopicsWithConfig(
            client.getAdmin(),
            {
              partitions: config.kafka.topicPartitions || 3,
              replicationFactor: Number(process.env.KAFKA_TOPIC_RF ?? 1),
            },
            true
          );
          logger.info("Topics reinitialized after reconnection");
        },
      },
    },
    logger,
  }
);

export default async function kafkaInit() {
  if (!config.kafka.enabled) {
    logger.warn("Kafka is disabled - skipping initialization");
    return;
  }

  try {
    await client.connect();
    
    await ensureTopicsWithConfig(
      client.getAdmin(),
      {
        partitions: config.kafka.topicPartitions || 3,
        replicationFactor: Number(process.env.KAFKA_TOPIC_RF ?? 1),
      },
      true
    );

    logger.info("Kafka initialized successfully");
  } catch (error) {
    logger.warn("Kafka initialization failed - will retry in background");
  }
}

// Export helpers
export const getKafkaStatus = () => client.getConnectionStatus();
export const getKafkaHealth = () => client.getHealthStatus();
export const disconnectKafka = () => client.disconnect();
export const producer = client.getProducer(); // Now throws if not connected
```

**Reduction**: From 217 lines to ~60 lines (73% reduction)

---

## Migration Guide

### Step 1: Update @gatwech/kafka Package

Implement the resilience features in the package as described above.

### Step 2: Update Application Dependencies

```bash
pnpm update @gatwech/kafka
```

### Step 3: Simplify Application Code

Replace custom resilience logic with package configuration:

1. Remove retry logic, circuit breaker, reconnection code
2. Configure via `KafkaClient` options
3. Use `client.getConnectionStatus()` for health checks
4. Use `client.getHealthStatus()` for monitoring

### Step 4: Update Health Check Endpoint

```typescript
// src/app.ts
app.get("/api/v1/payments/health", async (req: Request, res: Response) => {
  const detailed = req.query.detailed === "true";
  
  if (!detailed) {
    return res.status(200).json({
      status: "UP",
      timestamp: new Date().toISOString(),
    });
  }
  
  const { getKafkaHealth } = await import("@/infra/config/kafka");
  const kafkaHealth = getKafkaHealth();
  
  return res.status(200).json({
    status: kafkaHealth.status === 'healthy' ? 'UP' : 'DEGRADED',
    timestamp: new Date().toISOString(),
    services: {
      kafka: kafkaHealth,
    },
  });
});
```

---

## Testing Checklist for Package

- [ ] Unit tests for ConnectionManager retry logic
- [ ] Unit tests for CircuitBreaker behavior
- [ ] Unit tests for ReconnectionMonitor
- [ ] Integration tests for KafkaClient with resilience
- [ ] Test connection failure scenarios
- [ ] Test reconnection after Kafka restart
- [ ] Test circuit breaker activation
- [ ] Test health status reporting

---

## Backward Compatibility

Ensure the package remains backward compatible:

```typescript
// Default behavior: resilience enabled
new KafkaClient(clientId, brokers); // Resilience ON

// Opt-out if needed
new KafkaClient(clientId, brokers, {
  resilience: { enabled: false }
});
```

---

## Documentation Updates Needed

1. README with resilience features
2. API documentation for new options
3. Migration guide for existing users
4. Examples of common configurations
5. Troubleshooting guide for circuit breaker scenarios
