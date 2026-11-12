# Express Kafka Demo App

Production-ready Express.js application demonstrating `@gatwech/kafka` v0.0.9-beta.2 with full resilience features.

## Features

✅ **Non-blocking startup** - App starts immediately even if Kafka is down  
✅ **Auto-reconnection** - Background monitor reconnects automatically  
✅ **Circuit breaker** - Prevents cascading failures  
✅ **Health endpoints** - Kubernetes-ready probes  
✅ **Graceful shutdown** - Clean disconnection on SIGTERM/SIGINT  
✅ **Type-safe consumers** - Automatic schema validation with Zod  

## Quick Start

```bash
# Install dependencies
pnpm install

# Start Kafka (if using Docker)
docker-compose up -d

# Run the app
pnpm dev
```

The server will start on `http://localhost:3000` **immediately**, even if Kafka is unavailable.

## API Endpoints

### Health & Status

| Endpoint | Description | Returns |
|----------|-------------|---------|
| `GET /health` | Simple health check | Always 200 - app is running |
| `GET /health?detailed=true` | Detailed health with Kafka status | 200 with service details |
| `GET /ready` | Kubernetes readiness probe | 200 if ready, 503 if unhealthy |
| `GET /live` | Kubernetes liveness probe | 200 if alive, 503 if needs restart |
| `GET /kafka/status` | Kafka connection details | Connection status object |
| `GET /kafka/resilience` | Resilience features status | Circuit breaker & monitor state |
| `GET /metrics` | Kafka metrics | Publish/consume counters |

### Kafka Management

| Endpoint | Description |
|----------|-------------|
| `POST /kafka/reconnect` | Manually trigger reconnection |
| `POST /kafka/circuit-breaker/reset` | Reset circuit breaker |

### Event Publishing

| Endpoint | Body | Description |
|----------|------|-------------|
| `POST /events/user` | `{ userId, event }` | Publish user event |
| `POST /events/order` | `{ orderId, userId, amount, status }` | Publish order event |

## Example Usage

### Check Health

```bash
# Simple health check
curl http://localhost:3000/health

# Detailed health with Kafka status
curl http://localhost:3000/health?detailed=true
```

**Response when Kafka is down:**
```json
{
  "status": "DEGRADED",
  "timestamp": "2025-11-25T19:00:00.000Z",
  "services": {
    "kafka": {
      "status": "degraded",
      "connected": false,
      "uptime": null,
      "lastError": "Connection error: connect ECONNREFUSED"
    }
  }
}
```

**Response when Kafka is up:**
```json
{
  "status": "UP",
  "timestamp": "2025-11-25T19:00:00.000Z",
  "services": {
    "kafka": {
      "status": "healthy",
      "connected": true,
      "uptime": 45000,
      "lastError": null
    }
  }
}
```

### Publish Events

```bash
# Publish user event
curl -X POST http://localhost:3000/events/user \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123", "event": "login"}'

# Publish order event
curl -X POST http://localhost:3000/events/order \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-456",
    "userId": "user-123",
    "amount": 99.99,
    "status": "completed"
  }'
```

### Check Metrics

```bash
curl http://localhost:3000/metrics
```

**Response:**
```json
{
  "kafka": {
    "publishSuccess": 42,
    "publishFailures": 0,
    "connectionRetries": 2,
    "consumerConnections": 2
  }
}
```

## Resilience Features Demo

### Test Circuit Breaker

1. **Stop Kafka** to trigger failures:
   ```bash
   docker-compose stop kafka
   ```

2. **Try publishing events** - circuit will open after 5 failures:
   ```bash
   for i in {1..10}; do
     curl -X POST http://localhost:3000/events/user \
       -H "Content-Type: application/json" \
       -d "{\"userId\": \"user-$i\", \"event\": \"test\"}"
   done
   ```

3. **Check resilience status**:
   ```bash
   curl http://localhost:3000/kafka/resilience
   ```

4. **Start Kafka** again:
   ```bash
   docker-compose start kafka
   ```

5. **Wait for auto-reconnection** (60 seconds) or manually reset:
   ```bash
   curl -X POST http://localhost:3000/kafka/circuit-breaker/reset
   ```

### Expected Behavior

**When Kafka is Down:**
- ✅ App starts immediately (non-blocking)
- ✅ Health endpoint returns `200` with `DEGRADED` status
- ✅ Publish endpoints return `503 Service Unavailable`
- ✅ Background monitor retries every 60 seconds
- ✅ Circuit breaker opens after 5 failures

**When Kafka Comes Back:**
- ✅ Auto-reconnection within 60 seconds
- ✅ Topics re-provisioned automatically
- ✅ Circuit breaker closes
- ✅ Health endpoint returns `200` with `UP` status
- ✅ Publish endpoints work again

## Configuration

Environment variables:

```bash
# Kafka configuration
KAFKA_BROKERS=localhost:9092,localhost:9093
SERVICE_NAME=express-demo-service

# Server configuration
PORT=3000
```

## Kubernetes Deployment

### Deployment YAML

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: express-kafka-demo
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: express-kafka-demo:latest
        ports:
        - containerPort: 3000
        env:
        - name: KAFKA_BROKERS
          value: "kafka-service:9092"
        - name: SERVICE_NAME
          value: "express-kafka-demo"
        
        # Readiness probe - is pod ready to serve traffic?
        readinessProbe:
          httpGet:
            path: /ready
            port: 3000
          initialDelaySeconds: 5
          periodSeconds: 10
          failureThreshold: 3
        
        # Liveness probe - is pod alive?
        livenessProbe:
          httpGet:
            path: /live
            port: 3000
          initialDelaySeconds: 30
          periodSeconds: 30
          failureThreshold: 3
```

### Service YAML

```yaml
apiVersion: v1
kind: Service
metadata:
  name: express-kafka-demo
spec:
  selector:
    app: express-kafka-demo
  ports:
  - port: 80
    targetPort: 3000
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Express Application                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Health     │    │   Events     │    │  Management  │  │
│  │  Endpoints   │    │  Endpoints   │    │  Endpoints   │  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
│         │                   │                   │           │
│         └───────────────────┼───────────────────┘           │
│                             │                               │
│                    ┌────────▼────────┐                      │
│                    │  Kafka Client   │                      │
│                    │  (v0.0.9-beta.2)│                      │
│                    └────────┬────────┘                      │
│                             │                               │
│         ┌───────────────────┼───────────────────┐           │
│         │                   │                   │           │
│  ┌──────▼───────┐  ┌────────▼────────┐  ┌──────▼───────┐  │
│  │   Circuit    │  │  Reconnection   │  │  Auto-retry  │  │
│  │   Breaker    │  │    Monitor      │  │    Logic     │  │
│  └──────────────┘  └─────────────────┘  └──────────────┘  │
│                                                               │
└───────────────────────────┬───────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  Kafka Broker  │
                    └────────────────┘
```

## Client Configuration

The Kafka client is configured with full resilience features:

```typescript
const kafka = createKafkaClient({
  config: {
    clientId: 'express-demo-service',
    brokers: ['localhost:9092'],
    
    resilience: {
      enabled: true,
      
      // Circuit breaker prevents cascading failures
      circuitBreaker: {
        enabled: true,
        failureThreshold: 5,      // Open after 5 failures
        resetTimeoutMs: 30000,    // Test recovery after 30s
      },
      
      // Background reconnection monitor
      reconnection: {
        enabled: true,
        intervalMs: 60000,        // Check every 60s
        onReconnect: async () => {
          // Re-provision topics on reconnect
          await kafka.provisionTopics();
        },
      },
    },
    
    // Non-blocking startup
    autoConnectAsync: true,
  },
});
```

## Troubleshooting

### App Won't Start

**Symptom:** App crashes on startup

**Solution:** Check that you're not blocking on `await kafka.connect()` in `initKafka()`. With `autoConnectAsync: true`, connection happens in background.

### Circuit Breaker Stuck Open

**Symptom:** Publish endpoints return errors even though Kafka is back up

**Solution:** 
```bash
# Manually reset circuit breaker
curl -X POST http://localhost:3000/kafka/circuit-breaker/reset

# Or wait 30 seconds for automatic half-open state
```

### Too Many Retry Logs

**Symptom:** Logs flooded with connection retry attempts

**Solution:** This is normal when Kafka is down. The app is working correctly - it's just logging the retry attempts. To reduce noise, adjust log levels in production.

## Production Best Practices

1. **Use environment variables** for all configuration
2. **Set appropriate timeouts** for your infrastructure
3. **Monitor circuit breaker state** in your metrics
4. **Configure K8s probes** correctly (readiness vs liveness)
5. **Use structured logging** for better observability
6. **Set resource limits** in K8s deployment
7. **Enable horizontal pod autoscaling** based on metrics

## Related Documentation

- [Resilience Features](../../RESILIENCE.md) - Detailed resilience documentation
- [Consumer Example](./CONSUMER_EXAMPLE.md) - Type-safe consumer patterns
- [Main README](../../README.md) - Package documentation

## License

ISC
