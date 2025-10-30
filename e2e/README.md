E2E smoke test for the Kafka package

Prerequisites

- A Kafka broker reachable at localhost:9092 (or set KAFKA_BOOTSTRAP)

Run

```bash
# from repo root
pnpm --filter ./packages/kafka run test:e2e
```

You can override the topic or bootstrap server:

```bash
KAFKA_BOOTSTRAP=localhost:9092 E2E_TOPIC=my-e2e pnpm --filter ./packages/kafka run test:e2e
```
