import { describe, it, expect, vi, beforeEach } from "vitest";
import { createKafkaClient } from "../../src/client/core/create-client";

// Mock KafkaJS to control connection behavior
vi.mock("kafkajs", async () => {
	const actual = await vi.importActual<typeof import("kafkajs")>("kafkajs");

	const mockProducer = {
		connect: vi.fn().mockRejectedValue(new Error("Connection refused")),
		disconnect: vi.fn().mockResolvedValue(undefined),
		send: vi.fn(),
		on: vi.fn(),
	};

	const mockAdmin = {
		connect: vi.fn().mockResolvedValue(undefined),
		disconnect: vi.fn().mockResolvedValue(undefined),
		fetchTopicMetadata: vi.fn().mockResolvedValue({ topics: [] }),
	};

	class MockKafka {
		producer() {
			return mockProducer;
		}
		admin() {
			return mockAdmin;
		}
	}

	return {
		...actual,
		Kafka: MockKafka,
	};
});

/**
 * Integration tests for resilience features.
 *
 * These tests verify circuit breaker, error classification, health status, and metrics
 * using mocked KafkaJS to avoid timeout issues.
 */
describe("Resilience Integration Tests", () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	describe("Circuit Breaker Integration", () => {
		it("should open circuit after threshold failures", async () => {
			const kafka = createKafkaClient({
				config: {
					clientId: "test-circuit-breaker",
					brokers: ["localhost:9092"],
					topics: {},
					resilience: {
						enabled: true,
						retry: {
							maxAttempts: 1, // Fail fast
						},
						circuitBreaker: {
							enabled: true,
							failureThreshold: 3,
							resetTimeoutMs: 5000,
						},
						reconnection: {
							enabled: false,
						},
					},
				},
			});

			// Attempt 1: Should fail
			await expect(kafka.connect()).rejects.toThrow();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(false);

			// Attempt 2: Should fail
			await expect(kafka.connect()).rejects.toThrow();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(false);

			// Attempt 3: Should fail and open circuit
			await expect(kafka.connect()).rejects.toThrow();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(true);

			// Attempt 4: Should be blocked by circuit breaker
			await expect(kafka.connect()).rejects.toThrow(/Circuit breaker is open/);
		});

		it("should reset circuit on manual reset", async () => {
			const kafka = createKafkaClient({
				config: {
					clientId: "test-manual-reset",
					brokers: ["localhost:9092"],
					topics: {},
					resilience: {
						enabled: true,
						retry: {
							maxAttempts: 1,
						},
						circuitBreaker: {
							enabled: true,
							failureThreshold: 2,
						},
						reconnection: {
							enabled: false,
						},
					},
				},
			});

			// Trip the circuit
			await expect(kafka.connect()).rejects.toThrow();
			await expect(kafka.connect()).rejects.toThrow();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(true);

			// Manual reset
			kafka.resetCircuitBreaker();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(false);

			// Should allow connection attempt again
			await expect(kafka.connect()).rejects.toThrow();
			expect(kafka.getConnectionStatus().circuitBreakerOpen).toBe(false);
		});
	});

	describe("Error Classification Integration", () => {
		it("should only trip circuit on connection errors", async () => {
			const kafka = createKafkaClient({
				config: {
					clientId: "test-error-classification",
					brokers: ["localhost:9092"],
					topics: {},
					resilience: {
						enabled: true,
						retry: {
							maxAttempts: 1,
						},
						circuitBreaker: {
							enabled: true,
							failureThreshold: 2,
						},
						reconnection: {
							enabled: false,
						},
					},
				},
			});

			// Connection errors should trip the circuit
			await expect(kafka.connect()).rejects.toThrow();

			// Second connection error
			await expect(kafka.connect()).rejects.toThrow();
			const status2 = kafka.getConnectionStatus();

			// Circuit should open after threshold
			expect(status2.circuitBreakerOpen).toBe(true);
		});
	});

	describe("Health Status Integration", () => {
		it("should reflect circuit breaker state in health status", async () => {
			const kafka = createKafkaClient({
				config: {
					clientId: "test-health-status",
					brokers: ["localhost:9092"],
					topics: {},
					resilience: {
						enabled: true,
						retry: {
							maxAttempts: 1,
						},
						circuitBreaker: {
							enabled: true,
							failureThreshold: 2,
						},
						reconnection: {
							enabled: false,
						},
					},
				},
			});

			// Initial health - should be degraded
			const health1 = kafka.getHealthStatus();
			expect(health1.status).toBe("degraded");
			expect(health1.connected).toBe(false);

			// Trip the circuit
			await expect(kafka.connect()).rejects.toThrow();
			await expect(kafka.connect()).rejects.toThrow();

			// Health should reflect circuit breaker state
			const health2 = kafka.getHealthStatus();
			const connection2 = kafka.getConnectionStatus();
			expect(connection2.circuitBreakerOpen).toBe(true);
			expect(health2.status).toBe("unhealthy");
		});
	});

	describe("Metrics Integration", () => {
		it("should track connection retries in metrics", async () => {
			// Use fake timers to fast-forward through retry delays
			vi.useFakeTimers();

			const kafka = createKafkaClient({
				config: {
					clientId: "test-metrics",
					brokers: ["localhost:9092"],
					topics: {},
					resilience: {
						enabled: true,
						retry: {
							maxAttempts: 3, // Allow retries to test metrics
							initialDelayMs: 100,
							maxDelayMs: 200,
						},
						circuitBreaker: {
							enabled: false, // Disable to allow multiple attempts
						},
						reconnection: {
							enabled: false,
						},
					},
				},
			});

			const metrics1 = kafka.getMetrics();
			const initialRetries = metrics1.connectionRetries;

			// Attempt connection (will fail and retry)
			const connectPromise = kafka.connect().catch(() => {
				/**
				 * Expected to fail - suppress unhandled rejection warning
				 */
			});

			// Fast-forward through all retry delays (100ms + 200ms = 300ms, use 500ms to be safe)
			await vi.advanceTimersByTimeAsync(500);

			// Verify the connection failed
			await connectPromise;
			expect(kafka.getConnectionStatus().connected).toBe(false);

			const metrics2 = kafka.getMetrics();
			// Should have retried at least once (maxAttempts - 1)
			expect(metrics2.connectionRetries).toBeGreaterThanOrEqual(
				initialRetries + 2,
			);

			// Restore real timers
			vi.useRealTimers();
		});
	});
});
