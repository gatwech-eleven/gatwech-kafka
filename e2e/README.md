E2E smoke test for the Kafka package

Prerequisites

- A Kafka broker reachable at localhost:9092 (or set KAFKA_BOOTSTRAP)

Run (from this package)

```bash
pnpm run test:e2e
pnpm run test:e2e:service
pnpm run test:e2e:integration
pnpm run test:e2e:smoke
pnpm run test:e2e:express
```

You can override the topic or bootstrap server:

```bash
KAFKA_BOOTSTRAP=localhost:9092 E2E_TOPIC=my-e2e pnpm run test:e2e
KAFKA_BOOTSTRAP=localhost:9092 SERVICE_E2E_TOPIC=my-service-e2e pnpm run test:e2e:service
KAFKA_BOOTSTRAP=localhost:9092 INTEGRATION_TOPIC=user.registered pnpm run test:e2e:integration
KAFKA_BOOTSTRAP=localhost:9092 EXPRESS_INTEGRATION_TOPIC=user.registered pnpm run test:e2e:express
```
