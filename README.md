# payvo-kafka

Lightweight Kafka helpers for strongly-typed, validated, observable event publishing & consumption using KafkaJS + Zod.

## Quick Start (Preferred Factory)

```ts
import { Kafka } from "kafkajs";
import { z } from "@gatwech/utils/zod";
import { createServicePublisher } from "@payvo/kafka";

const kafka = new Kafka({ clientId: "users", brokers: ["localhost:9092"] });
const producer = kafka.producer();
await producer.connect();

const service = createServicePublisher(producer, "user-service-v1");

const UserRegistered = z.object({
  id: z.string(),
  email: z.string().email(),
  timestamp: z.number().optional(),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});

const publishUserRegistered = service(
  "user.registered",
  UserRegistered,
  (p) => p.id
);
await publishUserRegistered({ id: "123", email: "alice@example.com" });
```

Injected automatically: `timestamp` (epoch ms), `metadata.source`, `metadata.correlationId` (if absent). Headers include `correlation-id`, `event-type`, `timestamp`, optionally `schema-hash`.

## Event Factory (Low-Level)

Use `createEventPublisher` directly for one-off scripts or when you do not need service-wide metadata injection.

```ts
const publisher = createEventPublisher(
  producer,
  "user.registered",
  UserRegistered,
  (p) => p.id,
  "my-service"
);
await publisher({ id: "123", email: "a@example.com", timestamp: Date.now() });
```

## Service Factory Pattern

`createServicePublisher` (formerly `createServicePublisherFactory`) centralizes source + correlation id logic. Define publishers per topic via the returned function. The deprecated `createServicePublisherFactory` remains as an alias until the next major release.

## Consumer with Retry & DLQ

## Usage: Consumer with Retry & DLQ

Use `consumeWithRetryAndDLQ` to run consumers that:

- perform schema validation (if topic config provides a schema),
- retry failed message handling with exponential backoff, and
- publish permanently failing messages to a Dead Letter Queue (DLQ) topic.

Example:

```ts
import { consumeWithRetryAndDLQ, KafkaClient } from "@payvo/kafka";

const client = new KafkaClient("svc", ["localhost:9092"]);
await client.connect();

const topicConfig = {
  name: "user.registered" /* ...topic config with optional schema... */,
} as any;

await consumeWithRetryAndDLQ(
  client,
  "user-service-group",
  topicConfig,
  async ({ message }) => {
    const payload = message.value
      ? JSON.parse(message.value.toString())
      : undefined;
    // your message processing logic here
    // throw to trigger retry -> DLQ after configured attempts
  }
);
```

By default the DLQ topic is suffixed with `.dlq` (e.g. `user.registered.dlq`). DLQ messages include metadata about the original message, the consumer group and an error description.

## Connection Retry Helper

The repository includes `connectWithRetry`, a helper that retries an async connect function with exponential backoff. It is designed to handle transient DNS/network errors and avoid blocking service startup indefinitely.

Example:

```ts
import { connectWithRetry } from "@payvo/kafka";
import { KafkaClient } from "@payvo/kafka";

const client = new KafkaClient("svc", ["localhost:9092"]);
await connectWithRetry(() => client.connect(), {
  maxAttempts: 10,
  initialDelayMs: 500,
});
```

## Quick Tips

- **Send validated messages:** Use `createEventPublisher` for all outgoing events to guarantee runtime validation and consistent metadata.
- **Prefer factories for larger codebases:** Use `createServicePublisher` when many events share the same producer & source metadata. (`createServicePublisherFactory` deprecated)
- **DLQ handling:** Monitor the `.dlq` topics — a rising DLQ depth is the primary signal of processing issues.
- **Idempotency:** Consumers should be written idempotently (e.g., use upserts) because retries and rebalances can cause duplicate deliveries.

## Configuration & provisioning reminders

- Provision topics with `scripts/kafka-init.sh` or call `ensureTopicsWithConfig` during startup (opt-in via `PROVISION_KAFKA_TOPICS=1`).
- The E2E test harness lives in `e2e/run-e2e.ts` and demonstrates producer/consumer roundtrips including metadata and schema-hash headers.

If you want, I can expand the README with a small runnable example project or add a short troubleshooting section for common CI publish issues (NPM_TOKEN and GitHub Packages).

### Event Contract Requirements

All outbound events MUST include:

1. `timestamp: number` (epoch ms). If omitted in your payload the factories inject `Date.now()`. Use this for consistent, language‑neutral ordering and analytics.
2. `metadata.source: string` identifying the emitting service.
3. `metadata.correlationId: CorrelationId` (a branded string) either supplied by upstream trace context or auto‑generated via `generateCorrelationId("evt")`.

Branding utilities provide lightweight nominal typing:

- `CorrelationId` for trace continuity
- `ServiceSource` to distinguish origin identifiers from arbitrary strings
- `TopicName` for compile-time topic specificity (optional)
- `SchemaHash` for schema evolution tracking

Use helpers `asServiceSource("user-service-v3")`, `asTopicName("user.registered")` etc. Branding is erased at runtime, so interoperability with plain string APIs (KafkaJS) is unaffected.

If you evolve schemas add optional fields rather than removing required ones; consumers validate via Zod plus headers aiding traceability.

## Validation & Typed Producers

This package exposes small helpers to perform runtime validation of Kafka messages using Zod.

Usage examples

- Validate incoming consumer payloads:

```ts
import { validation } from "@payvo/kafka";
const res = validation.validateMessage(MySchema, parsedMessage, {
  topic: "user.registered",
});
if (!res.success) return; // already logged
const payload = res.data;
```

- Validate before producing:

```ts
import { producer } from "@payvo/kafka";
await producer.validateAndSend(kafkaClient, "user.registered", MySchema, {
  key: "",
  value: payload,
});
```

This package provides Kafka helpers, topic definitions and provisioning scripts used by the microservices.

## Provisioning

Quick patterns

1. Fast local developer flow (recommended)

- Start Kafka (docker-compose or local).
- Run the one-off init script once to create topics:

```bash
cd packages/kafka
bash scripts/kafka-init.sh
```

- Start your services normally (no provisioning during service startup):

```bash
pnpm --filter ./microservices/payvo-notification-service dev
```

2. One-shot provisioning via environment (opt-in)

- If you prefer the service to provision topics at startup, set the env var when starting the service:

```bash
PROVISION_KAFKA_TOPICS=1 pnpm --filter ./microservices/payvo-notification-service dev
```

This will run `ensureTopicsWithConfig` during startup; note that provisioning can add tens of seconds to startup because it waits for partition leaders and may attempt to alter topic configs.

3. Docker / CI provisioning

- For CI or containerized deploys, run the `packages/kafka/scripts/kafka-init.sh` as a job (CI step) or a short-lived init container before app containers start.

Example `docker-compose` snippet for an init service (adapt to your existing compose):

```yaml
	kafka-init:
		image: bash:5
		entrypoint: ["/bin/sh","-c","/workspace/packages/kafka/scripts/kafka-init.sh"]
		volumes:
			- ./:/workspace:cached
		depends_on:
			- kafka
		environment:
			BOOTSTRAP_SERVER: kafka:9092
```

Notes

- The script uses `kafka-topics.sh` if available or `docker exec kafka ...` if there is a container named `kafka`.
- The `ensureTopicsWithConfig` function in the library attempts to `alterConfigs` on topics — some brokers (or ACL configurations) may reject that call; the code logs a warning and continues.
- For fastest boot, provision once via the script and do not enable runtime provisioning in development.

## Deprecations

- `createServicePublisherFactory` -> use `createServicePublisher`.

## License

MIT (see repository root for details).
