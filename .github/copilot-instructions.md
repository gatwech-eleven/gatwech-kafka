# Kafka Client Improvement Plan

This document outlines the plan to enhance the Kafka client in our services, focusing on **reliability, type safety, observability, and testability**.

---

## 1. Producer Connection Enhancements

**Current Issue:**

- Producer may retry endlessly or block startup.
- Connection failure handling is inconsistent.

**Plan:**

- Implement bounded retries with exponential backoff.
- Set configurable connection timeout.
- Optionally allow async startup without blocking HTTP server.

**Test Plan:**

- Unit test: simulate connection failure and verify retries and errors.
- Metrics: ensure `connectionRetries` increments correctly.

---

## 2. Consumer Rebalance Optimizations

**Current Issue:**

- Consumer group rebalances may cause delays.
- Heartbeat/session timeouts not optimized.

**Plan:**

- Configure `sessionTimeout`, `heartbeatInterval`, and `rebalanceTimeout`.
- Ensure consumers can handle rebalances gracefully.

**Test Plan:**

- Simulate consumer group rebalances.
- Validate that messages are processed correctly post-rebalance.

---

## 3. Admin Client Lazy Connect

**Current Issue:**

- Admin client connects eagerly; slows startup.

**Plan:**

- Lazy initialization: connect only when needed.
- Cache single admin instance for repeated operations.

**Test Plan:**

- Verify connect/disconnect logic.
- Ensure multiple calls reuse the cached instance.

---

## 4. Startup Sequence Improvements

**Current Issue:**

- Kafka initialization can delay server startup.
- Optional topic provisioning blocks startup.

**Plan:**

- Async startup: HTTP server starts while Kafka connects.
- Topic provisioning controlled via `PROVISION_KAFKA_TOPICS=1`.

**Test Plan:**

- Verify server starts even if Kafka is delayed.
- Test topic provisioning flag behavior.

---

## 5. Metrics & Observability

**Plan:**

- Track key metrics:
  - `publishSuccess`, `publishFailures`
  - `connectionRetries`, `consumerConnections`
- Log significant events (producer connect/disconnect, consumer join, errors).

**Test Plan:**

- Simulate failed publishes and ensure metrics increment.
- Verify logs contain correct metadata (topic, key, timestamp).

---

## 6. Type Safety with Zod (New)

**Problem:** Runtime type errors in Kafka messages.

**Plan:**

- Define **Zod schemas** for each topic payload.
- Wrap producers to validate payloads before sending.
- Wrap consumers to validate messages on receive.
- Include default fields (`timestamp`) and auto-injected metadata (`correlationId`, `source`).

**Benefits:**

- Compile-time + runtime validation.
- Enforces consistent event contracts.
- Reduces runtime errors and unexpected payloads.

**Example:**

```ts
const UserRegisteredSchema = z.object({
  id: z.string(),
  email: z.string().email(),
  timestamp: z.date().default(() => new Date()),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});
```
