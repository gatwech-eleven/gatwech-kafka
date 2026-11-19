# @gatwech/kafka

A typed Kafka client for Node.js, on [KafkaJS](https://kafka.js.org) and [Zod](https://zod.dev). You declare your topics once, with a schema for each, and then `publish` and `consume` by name: events are checked before they are sent, handlers get typed events, and a message that can't be handled goes to a dead letter queue.

- **One declaration.** A topic's name, schema and partition key live in `defineTopics`.
- **Checked on both sides.** A publish is refused when the schema rejects the event, and a consumer sends a message the schema rejects to the DLQ.
- **Retries and a DLQ.** A handler that throws is called again with a back-off, then the message goes to `<topic>.dlq` with why it failed.
- **Connects when needed.** Every operation connects the client first, so a service can start before its brokers do.
- **No side effects.** Creating a client opens nothing and reads no environment variable.

## Install

```sh
pnpm add @gatwech/kafka
```

## Quick start

```ts
import { createKafka, defineTopics } from "@gatwech/kafka";
import { z } from "@gatwech/utils/zod";

const topics = defineTopics({
  example: {
    name: "example.created",
    schema: z.object({ id: z.string(), label: z.string() }),
    key: (event) => event.id,
  },
});

export const kafka = createKafka({
  clientId: "example-service",
  brokers: ["localhost:9092"],
  topics,
});

// Publish. The client connects and creates the topic on first use.
await kafka.publish({
  topic: "example",
  event: { id: "1", label: "first" },
});

// Consume. The handler's `event` is what the schema outputs.
await kafka.consume({
  topic: "example",
  handler: async ({ event }) => {
    console.log(event.id, event.label);
  },
});

// On shutdown
await kafka.disconnect();
```

## Topics

`defineTopics` declares the topics of a service. It reads nothing from the environment.

```ts
const topics = defineTopics({
  example: {
    name: "example.created", // the name on the broker
    schema: z.object({ id: z.string() }),
    key: (event) => event.id, // the partition key
    retentionMs: 86_400_000, // optional, 7 days by default
  },
});
```

- `publish` and `consume` name a topic by its key in this object (`"example"`), and TypeScript refuses a key that isn't there.
- Events with the same `key` go to one partition, so they are read in the order they were published.
- `namespace` on the client puts a prefix in front of every topic and DLQ name (`namespace: "prod"` makes `example.created` into `prod.example.created`), so environments can share a broker. A topic opts out with `namespaced: false`.
- `partitions`, `replicationFactor`, `retentionMs` and `cleanupPolicy` are optional on each topic. What they leave out comes from the client's `provisioning` option.

## Publishing

```ts
await kafka.publish({
  topic: "example",
  event: { id: "1", label: "first" },
  correlationId: "request-42", // optional, a new id by default
  headers: { "x-request-id": "r1" },
});

await kafka.publishBatch({
  topic: "example",
  events: [
    { id: "1", label: "first" },
    { id: "2", label: "second" },
  ],
});
```

- The event is checked by the topic's schema first. `publish` throws `INVALID_EVENT` for one it rejects, and `publishBatch` sends nothing when any event is rejected.
- The message body is the event with `metadata: { source, correlationId }` added. `source` is the client's `clientId`. The headers are `event-version`, `event-type`, `timestamp`, `correlation-id` and `source`, plus the ones you pass.
- A publish never creates a topic: `provision()` does, and connecting calls it. A topic that was deleted since throws `TOPIC_NOT_PROVISIONED`.

## Consuming

```ts
await kafka.consume({
  topic: "example",
  groupId: "example-service.example", // optional, `${clientId}.${topic}` by default
  handler: async ({ event, correlationId, source, partition, offset }) => {
    await store(event);
  },
  retry: { attempts: 3, backoffMs: 500 },
});
```

**Let errors throw.** When the handler throws, it is called again after `retry.backoffMs` (doubling each time), up to `retry.attempts` calls in all, and every failure is logged. Then the message goes to the DLQ. A handler that catches its own error hides the message from all of that, and its offset is committed.

**One group reads one topic.** A service that reads several topics calls `consume` once for each, and each gets its own group. A group with no committed offset starts where `fromBeginning` says (the end of the topic by default).

**Waiting for the brokers.** `consume` can be called at start, before anyone knows whether the brokers are up. It waits, and joins once a connection has provisioned the topics. If `disconnect()` comes first, it resolves with a consumer that does nothing.

`consume` resolves with `{ groupId, paused, resume, disconnect }`.

### The dead letter queue

A message goes to `<topic>.dlq` (the name of the topic on the broker, plus `.dlq`, or the `dlq.topic` you give) when:

- its handler failed on every attempt (`reason: "handler"`), or
- the topic's schema rejects it (`reason: "schema"`).

The DLQ entry is JSON, keyed by the message's key (or its offset when it has none):

```jsonc
{
  "timestamp": 1731800000000,
  "reason": "handler",
  "errorMessage": "…",
  "stackTrace": "…",
  "originalTopic": "example.created",
  "originalPartition": 0,
  "originalOffset": "105",
  "consumerGroup": "example-service.example",
  "originalKey": "1",
  "originalHeaders": { "correlation-id": "…" },
  "originalValue": { "id": "1", "label": "first" }
}
```

A schema failure has `validationIssues` in place of `stackTrace`. The message also carries the headers `x-original-topic`, `x-original-partition` and `x-original-offset`.

When the DLQ can't be published to either, the partition is paused, an `error` is logged and the offset is left uncommitted: the message is neither lost nor handled again until `consumer.resume()`. `consumer.paused()` says which partitions.

Monitor the DLQ's depth. It is the first sign that a handler is failing.

## Connecting

`createKafka(options)` opens nothing. Every operation connects the client first when it isn't connected, and throws `FAILED_TO_CONNECT` when no broker answers within `connect.attempts`. Calling `await kafka.connect()` at start is optional: it connects before the first operation needs it.

- A first connection that fails doesn't end the client. With `reconnection` on (the default), it keeps trying in the background, provisions the topics once it connects, and the consumers waiting for it join.
- `healthCheck()` says whether a broker answered, whether the client is connected, and the state of the circuit breaker. It never connects the client.
- `disconnect()` closes the producer and every consumer the client created. It never throws, so a shutdown carries on.
- `enabled: false` turns the client off: `connect`, `provision`, `disconnect` and `consume` do nothing, and `publish` logs at `warn` and sends nothing.

### A circuit breaker on publishes

After `circuitBreaker.failureThreshold` publishes in a row fail, publishes throw `CIRCUIT_OPEN` without calling the broker. After `resetTimeoutMs`, one publish is let through: if it works, the circuit closes.

## Types

`$Infer` has the event of each topic, by key, as a handler receives it:

```ts
type Example = typeof kafka.$Infer.example; // { id: string; label: string }
```

## Options

```ts
createKafka({
  clientId: "example-service",
  brokers: ["localhost:9092"],
  topics,
  namespace: "prod", // default: none
  enabled: true, // default: true
  ssl: true, // default: false
  sasl: { mechanism: "plain", username, password },
  compression: "gzip", // default: "gzip"
  connect: { attempts: 5, retryDelayMs: 2_000 },
  provisioning: { partitions: 3, replicationFactor: 1, waitForLeadersMs: 15_000 },
  circuitBreaker: { enabled: true, failureThreshold: 5, resetTimeoutMs: 30_000 },
  reconnection: { enabled: true, intervalMs: 60_000 },
  logger: { level: "info", tag: "example-service" },
});
```

Each option is documented in its type, with its default.

## Errors

The package throws one error, `KafkaError`. Its `message` is one of `KAFKA_ERROR_CODES`, and its `cause` is the error behind it: the broker's, the handler's or the schema's.

```ts
import { KAFKA_ERROR_CODES, KafkaError } from "@gatwech/kafka";

try {
  await kafka.publish({ topic: "example", event });
} catch (error) {
  if (
    error instanceof KafkaError &&
    error.message === KAFKA_ERROR_CODES.INVALID_EVENT
  ) {
    // error.cause is the ZodError
  }
}
```

| Code | When |
| --- | --- |
| `TOPIC_NAME_REQUIRED`, `TOPIC_SCHEMA_REQUIRED`, `TOPIC_KEY_REQUIRED`, `TOPIC_NAME_DUPLICATED`, `INVALID_TOPIC_OPTION` | `defineTopics` or `createKafka` is given a topic it can't use |
| `BROKERS_REQUIRED` | `brokers` is empty |
| `TOPIC_NOT_DECLARED` | `publish` or `consume` names a topic the client doesn't have |
| `INVALID_EVENT` | the topic's schema rejects an event |
| `FAILED_TO_CONNECT` | no broker answered within `connect.attempts` |
| `CIRCUIT_OPEN` | publishes are failing, and the circuit refuses them |
| `TOPIC_NOT_PROVISIONED` | the topic is gone from the broker |
| `FAILED_TO_PUBLISH` | the broker refused the message |
| `FAILED_TO_START_CONSUMER` | a consumer couldn't connect, join its group or subscribe |
| `INVALID_RETRY`, `INVALID_HEARTBEAT_INTERVAL` | consumer settings that would get it removed from its group |
| `FAILED_TO_PUBLISH_TO_DLQ` | the DLQ is unreachable, so the partition is paused |

## Testing

The package's own tests run against a real broker. Start one with the repository's `docker-compose.yml`, with topic auto-creation off as in `.github/workflows/ci.yml`, and point `KAFKA_BROKERS` at it:

```sh
KAFKA_BROKERS=localhost:9092 pnpm test
```

Test your own code the same way, against a real Kafka (for example with Testcontainers): serialisation, rebalances and the DLQ are what a mock can't show. A client with `enabled: false` runs code that publishes without a broker.

## Upgrading from 0.x

1.0 replaces the 0.x API. Nothing is deprecated: it is gone.

| 0.x | 1.0 |
| --- | --- |
| `new KafkaClient(...)`, `createKafkaClient({ config, extensions })` | `createKafka(options)` |
| `createServicePublisherFactory`, `createEventPublisher` | `kafka.publish({ topic, event })`, with the topic declared in `defineTopics` |
| `consumeWithRetryAndDLQ(client, groupId, topic, handler, opts)` | `kafka.consume({ topic, groupId, handler, retry })` |
| `maxRetries`, `retryBackoffMs`, `dlqEnabled`, `dlqSuffix` | `retry.attempts`, `retry.backoffMs`. A DLQ is always on, named `<topic>.dlq` unless `dlq.topic` says otherwise |
| `connectWithRetries`, `maxConnectAttemptTimeMs`, `connectAsync`, `connectWithRetry` | `connect.attempts` and `connect.retryDelayMs`, and `reconnection`. Operations connect the client themselves |
| `topicProvisioningExtension`, `PROVISION_KAFKA_TOPICS`, `scripts/kafka-init.sh` | `provision()`, which connecting calls. Topics are created from `defineTopics` |
| `client.healthCheck()` | `kafka.healthCheck()`, which also reports the circuit |
| `partitionAnalysisExtension` | removed |
| a `topic()` helper in each service | the client's `namespace` |
| `KAFKA_ENABLED` wired by hand | the client's `enabled` |
| `z.infer<typeof Schema>` for a handler's event | `typeof kafka.$Infer.<topic>` |
| `BROKER_UNREACHABLE`, `PUBLISH_FAILED`, `EVENT_REJECTED_BY_SCHEMA`, `CONSUMER_START_FAILED` (0.1 betas), and `NOT_CONNECTED` | `FAILED_TO_CONNECT`, `FAILED_TO_PUBLISH`, `INVALID_EVENT`, `FAILED_TO_START_CONSUMER`, and no code for `NOT_CONNECTED`: operations connect the client |

A consumer that hasn't moved still reads what 1.0 publishes: the body and headers keep the shape the 0.x publishers wrote. Keep a consumer's `groupId` when you move it, so it keeps its committed offsets.

## License

ISC
