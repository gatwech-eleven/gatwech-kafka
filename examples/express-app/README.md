# Express Demo

Express.js application demonstrating the `@gatwech/kafka` package features.

## Features

- ✅ Automatic retry with exponential backoff
- ✅ Circuit breaker protection
- ✅ Background reconnection monitoring
- ✅ Health check endpoints
- ✅ Kafka status monitoring
- ✅ Manual reconnection API
- ✅ Circuit breaker reset
- ✅ Event publishing
- ✅ Metrics collection
- ✅ Graceful shutdown

## Code Structure

```
express-app/
├── src/
│   ├── client.ts      # Kafka client configuration
│   ├── topics.ts      # Topic definitions
│   └── index.ts       # HTTP server and API endpoints
├── package.json       # Dependencies and scripts
└── README.md          # This file
```

## Prerequisites

- Kafka broker running (default: `localhost:9092`)
- Node.js and pnpm installed

## Setup

From the workspace root:

```bash
# Install dependencies
pnpm install

# The express-app will use the local @gatwech/kafka package
```

## Running

### Start Kafka

```bash
# From packages/kafka directory
docker-compose up -d
```

### Run the Demo

```bash
# From this directory (examples/express-app)
pnpm dev

# Or from workspace root
pnpm --filter @examples/express-demo dev
```

The server will start on `http://localhost:3000`

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Comma-separated Kafka brokers | `localhost:9092` |
| `SERVICE_NAME` | Service name for client ID | `express-demo-service` |
| `PORT` | Express server port | `3000` |

Example:

```bash
KAFKA_BROKERS=localhost:9092 PORT=3000 pnpm dev
```

## API Endpoints

### Health Check

```bash
# Simple
curl http://localhost:3000/health

# Detailed with Kafka status
curl http://localhost:3000/health?detailed=true
```

### Kafka Status

```bash
curl http://localhost:3000/kafka/status
```

### Publish Event

```bash
curl -X POST http://localhost:3000/events/user \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123", "event": "login"}'
```

### Manual Reconnection

```bash
curl -X POST http://localhost:3000/kafka/reconnect
```

### Reset Circuit Breaker

```bash
curl -X POST http://localhost:3000/kafka/circuit-breaker/reset
```

### Metrics

```bash
curl http://localhost:3000/metrics
```

## Testing Resilience

### Test Automatic Retry

1. Stop Kafka: `docker-compose down`
2. Start the app: `pnpm dev`
3. Watch retry attempts in logs

### Test Background Reconnection

1. Start app with Kafka running
2. Stop Kafka: `docker-compose down`
3. Wait 60 seconds
4. Watch background reconnection attempts

### Test Circuit Breaker

After multiple failures, check status:

```bash
curl http://localhost:3000/kafka/status
# "circuitBreakerOpen": true

# Reset it
curl -X POST http://localhost:3000/kafka/circuit-breaker/reset
```


## Development

```bash
# Type check
pnpm typecheck

# Run in development mode
pnpm dev
```

## See Also

- [EXPRESS_DEMO.md](../EXPRESS_DEMO.md) - Detailed API documentation
- [Resilience Quick Start](../../RESILIENCE_QUICKSTART.md) - Configuration guide
- [@gatwech/kafka](../../) - Main package
