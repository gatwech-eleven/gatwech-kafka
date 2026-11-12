import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createCircuitBreaker } from "../../../src/client/core/circuit-breaker";

describe("CircuitBreaker", () => {
	beforeEach(() => {
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	describe("Initialization", () => {
		it("should start in closed state", () => {
			const breaker = createCircuitBreaker({ threshold: 5 });

			expect(breaker.isOpen()).toBe(false);
			expect(breaker.getFailureCount()).toBe(0);
		});

		it("should accept custom threshold", () => {
			const breaker = createCircuitBreaker({ threshold: 3 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(false);

			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);
		});

		it("should accept custom reset timeout", () => {
			const breaker = createCircuitBreaker({ threshold: 2, resetTimeoutMs: 5000 });

			expect(breaker.isOpen()).toBe(false);
		});
	});

	describe("Failure Recording", () => {
		it("should increment failure count on recordFailure", () => {
			const breaker = createCircuitBreaker({ threshold: 5 });

			breaker.recordFailure();
			expect(breaker.getFailureCount()).toBe(1);

			breaker.recordFailure();
			expect(breaker.getFailureCount()).toBe(2);
		});

		it("should open circuit when threshold is reached", () => {
			const breaker = createCircuitBreaker({ threshold: 3 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(false);

			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);
		});

		it("should stay open after exceeding threshold", () => {
			const breaker = createCircuitBreaker({ threshold: 2 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);
		});
	});

	describe("Success Recording", () => {
		it("should reset failure count on recordSuccess", () => {
			const breaker = createCircuitBreaker({ threshold: 5 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.getFailureCount()).toBe(2);

			breaker.recordSuccess();
			expect(breaker.getFailureCount()).toBe(0);
		});

		it("should close circuit on recordSuccess", () => {
			const breaker = createCircuitBreaker({ threshold: 2 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			breaker.recordSuccess();
			expect(breaker.isOpen()).toBe(false);
		});
	});

	describe("Half-Open State (Automatic Recovery)", () => {
		it("should remain open during reset timeout", () => {
			const breaker = createCircuitBreaker({
				threshold: 2,
				resetTimeoutMs: 10000,
			});

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Advance time by 5 seconds (less than timeout)
			vi.advanceTimersByTime(5000);
			expect(breaker.isOpen()).toBe(true);
		});

		it("should allow request after reset timeout (Half-Open)", () => {
			const breaker = createCircuitBreaker({
				threshold: 2,
				resetTimeoutMs: 10000,
			});

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Advance time past the timeout
			vi.advanceTimersByTime(10001);
			expect(breaker.isOpen()).toBe(false); // Half-Open: allows test request
		});

		it("should close permanently if test request succeeds", () => {
			const breaker = createCircuitBreaker({ threshold: 2, resetTimeoutMs: 5000 });

			// Trip the breaker
			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Wait for Half-Open
			vi.advanceTimersByTime(5001);
			expect(breaker.isOpen()).toBe(false);

			// Test request succeeds
			breaker.recordSuccess();
			expect(breaker.isOpen()).toBe(false);
			expect(breaker.getFailureCount()).toBe(0);
		});

		it("should reopen if test request fails in Half-Open state", () => {
			const breaker = createCircuitBreaker({ threshold: 2, resetTimeoutMs: 5000 });

			// Trip the breaker
			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Wait for Half-Open
			vi.advanceTimersByTime(5001);
			expect(breaker.isOpen()).toBe(false);

			// Test request fails
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);
		});

		it("should use default timeout of 30 seconds if not specified", () => {
			const breaker = createCircuitBreaker({ threshold: 2 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// 29 seconds - still open
			vi.advanceTimersByTime(29000);
			expect(breaker.isOpen()).toBe(true);

			// 31 seconds - Half-Open
			vi.advanceTimersByTime(2000);
			expect(breaker.isOpen()).toBe(false);
		});
	});

	describe("Manual Reset", () => {
		it("should reset circuit manually", () => {
			const breaker = createCircuitBreaker({ threshold: 2 });

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			breaker.reset();
			expect(breaker.isOpen()).toBe(false);
			expect(breaker.getFailureCount()).toBe(0);
		});

		it("should allow immediate requests after manual reset", () => {
			const breaker = createCircuitBreaker({
				threshold: 2,
				resetTimeoutMs: 10000,
			});

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Manual reset (no need to wait for timeout)
			breaker.reset();
			expect(breaker.isOpen()).toBe(false);
		});
	});

	describe("Integration Scenarios", () => {
		it("should handle multiple failure-recovery cycles", () => {
			const breaker = createCircuitBreaker({ threshold: 3, resetTimeoutMs: 1000 });

			// First cycle: fail and recover
			breaker.recordFailure();
			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			vi.advanceTimersByTime(1001);
			breaker.recordSuccess();
			expect(breaker.isOpen()).toBe(false);

			// Second cycle: fail again
			breaker.recordFailure();
			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(true);

			// Recover again
			vi.advanceTimersByTime(1001);
			breaker.recordSuccess();
			expect(breaker.isOpen()).toBe(false);
		});

		it("should not open if failures are below threshold", () => {
			const breaker = createCircuitBreaker({ threshold: 5 });

			breaker.recordFailure();
			breaker.recordFailure();
			breaker.recordSuccess(); // Reset

			breaker.recordFailure();
			breaker.recordFailure();
			expect(breaker.isOpen()).toBe(false);
		});
	});
});
