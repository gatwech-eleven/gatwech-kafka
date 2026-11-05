# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Consumer**: `consumeWithRetryAndDLQ` no longer loses a message when the DLQ publish fails. It rethrows, so `eachMessage` rejects, the offset isn't committed and the message is redelivered. This applies to messages that fail schema validation and to messages whose handler failed every retry.

## [0.0.8-beta.1] - 2025-11-25

### Added
- **Configuration**: Added direct support for `ssl`, `sasl`, `connectionTimeout`, `authenticationTimeout`, `reauthenticationThreshold`, and `enforceRequestTimeout` in `KafkaClientOptions`.
- **Documentation**: Added comprehensive Clean Code TypeScript guidelines document for the team.
- **Type Safety**: Improved Logger interface to use `Record<string, unknown>` instead of `any` for better type safety.

### Changed
- **Tests**: Moved E2E tests from `e2e/` to `__tests__/e2e/` for better project structure.
- **Extensions**: Renamed `partition-calc` to `partition-analysis` and updated export to `partitionAnalysisExtension`.

### Code Compliance
- **Type Safety**: Added proper ESLint disable comments with justifications for all legitimate `any` usage:
  - `connect-with-retry.ts`: Error handling for KafkaJS errors
  - `consumer-extension.ts`: Handler parameter for function overload implementation
  - `consume-with-dlq.ts`: Handler parameter for function overload implementation
  - `types/kafka.ts`: KafkaJS partitioner signature compatibility
- **Documentation**: All `any` types now include clear explanations per Clean Code guidelines.
- **Verification**: All 143 tests passing, TypeScript compilation clean, linter passing.

### Refactoring

#### Clean Code Refactoring - Phase 1 & 2

**Phase 1: Quick Wins (Comments, Logs, Magic Numbers)**

- **Examples**
  - Replaced `console.log/error` with `logger` in `examples/client.ts` and `examples/typed-consumer-example.ts`
  - Extracted magic numbers to named constants (`DEFAULT_PORT`, `LOG_LEVEL_INFO`, `SEVEN_DAYS_MS`, `THIRTY_DAYS_MS`)
  - Removed positional marker comments and redundant comments

- **Source Code**
  - Replaced `console.log/warn` with `logger` in `src/extensions/partition-calc.ts`
  - Extracted magic numbers to constants:
    - `CORRELATION_ID_LENGTH` in `src/utils/helpers.ts`
    - `MAX_RETRY_DELAY_MS` in `src/client/core/kafka.ts`
    - `DEFAULT_SKEW_THRESHOLD` in `src/extensions/partition-calc.ts`
    - `RETRY_DELAY_BASE_MS`, `RETRY_DELAY_CAP_MS`, `WAIT_FOR_LEADERS_DEFAULT_MS` in `src/config/ensure-topics.ts`
  - Removed "what" comments from `src/config/ensure-topics.ts`

**Phase 2: Type Safety & Naming**

- **Source Code**
  - Renamed `TReadyToPublishMessage` to `ReadyToPublishMessage` in `src/client/producers/generic-producer.ts` (removed Hungarian notation)

- **Tests**
  - Removed `as any` type assertions in test files
  - Replaced unsafe type casts with `vi.mocked()` for better type safety
  - Extracted magic number `RETRY_BACKOFF_MS` in `__tests__/consumer/consume-with-dlq.test.ts`
  - Added proper type imports (`KafkaClient`, `Producer`) in test files

These changes improve code maintainability, readability, and type safety according to the clean code guidelines.
