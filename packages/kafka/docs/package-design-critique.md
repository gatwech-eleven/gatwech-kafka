# Package Design Critique — @gatwech/kafka

Date: 2025-11-22

This document provides a concise, high-level critique of the package design for `@gatwech/kafka`. It covers architecture patterns, strengths, weaknesses/risks, and actionable recommendations prioritized for implementation.

---

## Summary

`@gatwech/kafka` is an opinionated, well-typed Kafka client library built around KafkaJS and Zod. Its core focus is message publishing and consumption with built-in reliability patterns (retry, DLQ), schema validation, and runtime tooling for topic provisioning and health checks. The package is cohesive and well-tested, with a discoverable public API and practical production-minded features.

---

## Architecture & Patterns (high level)

- Modular structure: `client`, `producers`, `consumers`, `config`, `extensions`, `schemas`, `types`, and `utils`.
- Extension system: `createKafkaClient(...)` accepts `KafkaExtension` plugins; extensions run `setup(client)` and return a public API which is mixed into the client instance.
- Factory-based publishers: `createServicePublisherFactory` and `createEventPublisher` separate high-level and low-level publishing scenarios.
- Schema-first validation: Zod schemas are used for runtime validation and TypeScript inference.
- Reliability primitives: retry + DLQ patterns, admin/topic provisioning scripts, and options for broker readiness polling.

---

## Strengths

- **Focused Responsibility**: Clear purpose — typed Kafka messaging toolkit for microservices.
- **Strong Typing + Runtime Safety**: Zod schemas with `z.infer` give both runtime validation and ergonomic typing.
- **Good Directory Organization**: Logical separation of producers, consumers, client core and extensions improves maintainability.
- **Extension System**: Flexible mechanism to opt-in features (provisioning, health checks) without bloating the base client.
- **Testing & E2E Support**: E2E scripts and recommendations for Testcontainers indicate good test ergonomics.
- **Packaging**: `exports` map and dual outputs (`module` / `main`) are modern and consumer-friendly when built correctly.

---

## Weaknesses & Risks

- **Extension API Collisions**: `Object.assign(client, ext)` can silently overwrite base or other extension methods, causing subtle runtime bugs.
- **Large Public Surface Area**: Many top-level exports increase the chance of consumers importing internal modules directly (fragile coupling).
- **Opinionation without Safe Defaults**: Idempotence / acks / producer best-practices are recommended but not enforced by default.
- **Build Verification Reliance**: The build (`tsdown`) must produce both CJS and ESM artifacts and accurate `types`. Missing CI checks may cause publish-time breakage.
- **Deprecated API Noise**: Deprecated exports are kept for compatibility; without runtime warnings they can be used unknowingly.
- **Schema Evolution Tooling Gap**: Zod schemas are great for local validation but there is no optional integration with a Schema Registry to coordinate cross-team evolution.

---

## Actionable Recommendations (prioritized)

1. **Prevent Extension Collisions & Namespace Extensions** (High)

   - At `createKafkaClient` runtime, validate extension `name` uniqueness and throw on collisions.
   - Mix extension APIs under a reserved `extensions` object on the client, e.g. `client.extensions.topicProvisioning.provisionTopics()`.
   - Add TS helper types so extension APIs are available as `client.extensions.<name>`.

2. **Offer an Opinionated Producer Factory** (High)

   - Provide `createOpinionatedProducer()` or a configurable factory that defaults to `idempotent: true`, `acks: 'all'`, reasonable retry/backoff.
   - Document how to opt-out and the trade-offs.

3. **Add Build & Export Verification in CI** (Medium)

   - Add a CI job to run `pnpm build` then a small node script that imports `./dist/index.cjs` and the subpath exports to ensure runtime resolvability.
   - Verify `.d.ts` generation and the `exports` map in a smoke import test.

4. **Emit Runtime Deprecation Warnings** (Low–Medium)

   - When deprecated functions are invoked, emit `console.warn` with a migration hint and target removal version.
   - Optionally add a `--suppress-deprecation-warnings` env var for CI clarity.

5. **Provide Optional Schema Registry Integration** (Medium)

   - Offer a plugin that can (optionally) register Zod schemas to a registry or compute a stable schema-hash and include it in message headers.
   - This helps consumers detect incompatible schema changes and simplifies cross-team coordination.

6. **Reduce Top-Level Export Noise** (Low)

   - Group exports under clear namespaces such as `client`, `producer`, `consumer`, `config`, `extensions` for easier discoverability and to discourage fragile imports from deep internal paths.

7. **Automated Migration Helpers** (Low)

   - Add scripts or codemod hints in `scripts/` to update common deprecated names in codebases (example: `waitForBrokerBeforeConnect` -> `connectWithRetries`).

8. **Add Integration Tests for Rebalances and Backpressure** (Medium)
   - Create tests that simulate partition leadership changes and long-running handlers to verify offset-commit, rejoin behavior, and consumer stability under backpressure.

---

## Suggested First Pull Request

To make an immediate safety improvement, implement 1) runtime extension-name uniqueness checks and 2) namespace extension APIs under `client.extensions`. This is a small, low-risk change with high value because it prevents accidental method collisions and improves discoverability.

If you want, I can create that PR now (two changes):

- Modify `createKafkaClient` to validate extension names and assign extension APIs to `client.extensions[name]` instead of mixing at top-level.
- Add TypeScript signatures so `ModularKafkaClient` exposes `extensions` with typed extension return shapes.

---

---

_End of critique._
