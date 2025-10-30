# payvo-kafka

## Validation & typed producers

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

# payvo-kafka
