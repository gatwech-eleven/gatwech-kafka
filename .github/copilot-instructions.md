Kafka Client Improvement Plan

This document outlines the plan to enhance the Kafka client in our services, focusing on reliability, type safety, observability, and testability to build a robust, production-grade system.

1. Producer Connection Enhancements

Current Issue:

Producer may retry endlessly or block startup.

Connection failure handling is inconsistent.

Plan:

Implement bounded retries with exponential backoff.

Set configurable connection timeout.

Optionally allow async startup without blocking HTTP server.

Test Plan:

Unit test: simulate connection failure and verify retries and errors.

Metrics: ensure connectionRetries increments correctly.

2. Consumer Rebalance Optimizations

Current Issue:

Consumer group rebalances may cause delays.

Heartbeat/session timeouts not optimized.

Plan:

Configure sessionTimeout, heartbeatInterval, and rebalanceTimeout.

Ensure consumers can handle rebalances gracefully.

Test Plan:

Simulate consumer group rebalances.

Validate that messages are processed correctly post-rebalance.

3. Admin Client Lazy Connect

Current Issue:

Admin client connects eagerly; slows startup.

Plan:

Lazy initialization: connect only when needed.

Cache single admin instance for repeated operations.

Test Plan:

Verify connect/disconnect logic.

Ensure multiple calls reuse the cached instance.

4. Startup Sequence Improvements

Current Issue:

Kafka initialization can delay server startup.

Optional topic provisioning blocks startup.

Plan:

Async startup: HTTP server starts while Kafka connects.

Topic provisioning controlled via PROVISION_KAFKA_TOPICS=1.

Test Plan:

Verify server starts even if Kafka is delayed.

Test topic provisioning flag behavior.

5. Metrics & Observability

Plan:

Track key metrics:

publishSuccess, publishFailures

connectionRetries, consumerConnections

Log significant events (producer connect/disconnect, consumer join, errors).

Test Plan:

Simulate failed publishes and ensure metrics increment.

Verify logs contain correct metadata (topic, key, timestamp).

6. Type Safety with Zod

Problem: Runtime type errors in Kafka messages.

Plan:

Define Zod schemas for each topic payload.

Wrap producers to validate payloads before sending.

Wrap consumers to validate messages on receive.

Include default fields (timestamp) and auto-injected metadata (correlationId, source).

Benefits:

Compile-time + runtime validation.

Enforces consistent event contracts.

Reduces runtime errors and unexpected payloads.

Example:

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

7. Consumer Error Handling & Dead Letter Queues (DLQ)

Problem:

The current plan ensures consumers run, but doesn't define what to do when a single message cannot be processed (e.g., invalid data that passed Zod, a temporary downstream outage, or a bug).

A failing message can block its entire partition if it's retried indefinitely, halting all processing for that partition.

Plan:

Implement a finite-retry-with-backoff strategy within the consumer logic.

After a configurable number (e.g., 3) of retries, the "poison pill" message is considered non-processable.

The consumer will then publish this failing message to a Dead Letter Queue (DLQ) topic (e.g., user-registered.dlq).

The DLQ message should be enriched with metadata about the failure (error message, stack trace, original topic, partition, offset, and consumer group).

Benefits:

Unblocks the partition: Allows the consumer to move on and process subsequent, healthy messages.

Data preservation: No data is lost. Failing messages are safely stored for later inspection, reprocessing, or alerting.

High-level observability: The depth of the DLQ is a critical metric for monitoring system health.

8. Message Guarantees & Idempotence

Problem:

The default Kafka configuration provides "at-least-once" delivery, which can lead to duplicate processing during consumer rebalances or producer retries.

The plan doesn't explicitly state the desired processing guarantee.

Plan:

Producer:

Enable the idempotent producer (enable.idempotence=true). This ensures producer retries (due to network issues) do not create duplicate messages in the topic.

Set acks=all to ensure the producer only receives a success confirmation after the message is written to all in-sync replicas, guaranteeing durability.

Consumer:

Disable auto-commit (enable.auto.commit=false).

Implement manual offset commits. The consumer should only commit an offset after the message has been fully and successfully processed (e.g., written to the database).

Design consumer logic to be idempotent. This means that processing the same message multiple times has no additional effect (e.g., use INSERT ON CONFLICT or UPSERT in the database).

Benefits:

Creates a highly reliable "effectively-once" processing system.

Prevents data duplication and ensures data integrity, which is critical for financial or event-sourcing systems.

9. Data Governance & Schema Evolution

Problem:

Your Zod plan is excellent for type safety at the code level.

It does not solve schema evolution. What happens when the UserRegistered event needs a new v2 field? A consumer expecting v1 might break, or a producer sending v1 will fail v2 validation.

Plan:

Integrate a Schema Registry (e.g., Confluent Schema Registry, AWS Glue, or similar).

Define schemas (using Avro, Protobuf, or JSON Schema) in the registry instead of just in Zod.

Set compatibility rules (e.g., BACKWARD_COMPATIBLE) to enforce that new schemas can be read by old consumers.

Producers validate and register their schema, and consumers fetch the schema by ID to deserialize.

Zod can still be used to generate static types from the registered schemas, giving you both runtime validation and code-level type safety.

Benefits:

Decouples producers and consumers. A producer can update a schema (e.g., add an optional field) without breaking any consumers.

Enforces data contracts at the infrastructure level, preventing "rogue" services from sending invalid data.

Provides a centralized "source of truth" for all event schemas in your organization.

10. Advanced Observability: Tracing & Consumer Lag

Problem:

The current metrics plan is good for counters but misses end-to-end visibility and the single most important consumer metric.

Plan:

Metrics: Add monitoring for consumer-group-lag (per partition). This metric shows how many messages "behind" your consumer is from the end of the log. It is the primary indicator of consumer health and processing delays.

Tracing: Implement Distributed Tracing (e.g., OpenTelemetry).

The producer will inject the current trace context (trace ID, span ID) into the Kafka message headers.

The consumer will extract this context from the headers, creating a new child span.

Benefits:

Consumer Lag: Allows you to set precise alerts (e.g., "Alert if consumer lag > 1000 messages").

Tracing: Gives you a single, end-to-end view of a request as it flows from one service, through Kafka, to a consuming service. This is invaluable for debugging and performance analysis.

11. Robust Integration Testing

Problem:

Unit tests with mocks are good but cannot catch configuration errors, serialization bugs, or race conditions that only appear when talking to a real broker.

Plan:

Integrate Testcontainers into your CI/CD pipeline.

For integration tests, programmatically spin up a real Kafka broker (or a compatible alternative like Redpanda) in a Docker container.

Run your tests (producing, consuming, checking DLQs) against this real broker.

The container is automatically torn down after the test.

Benefits:

Provides extremely high confidence that your client code works with a real Kafka cluster.

Catches (de)serialization, connection, retry, and rebalance issues before they hit production.
