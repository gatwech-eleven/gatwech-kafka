# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

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
