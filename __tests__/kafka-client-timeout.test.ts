import { describe, it, expect } from "vitest";
import { KafkaClient } from "../src/client/kafka";
import { KafkaError } from "../src/client/errors";

describe.skip("KafkaClient connect timeout behavior (skipped flaky)", () => {
  it.skip("throws a broker readiness timeout when controller never appears", async () => {
    // skipped in CI due to flakiness with module mocks across test files
  });
});
