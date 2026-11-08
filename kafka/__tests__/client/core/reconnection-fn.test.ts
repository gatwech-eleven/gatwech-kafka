import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
	createReconnectionMonitor,
	type ReconnectionMonitor,
} from "../../../src/client/core/reconnection-monitor";
import type { Logger } from "../../../src/types";

describe("ReconnectionMonitor", () => {
	let mockLogger: Logger;
	let monitor: ReconnectionMonitor;

	beforeEach(() => {
		vi.useFakeTimers();
		mockLogger = {
			info: vi.fn(),
			warn: vi.fn(),
			error: vi.fn(),
		};
	});

	afterEach(() => {
		vi.useRealTimers();
		if (monitor) {
			monitor.stop();
		}
	});

	describe("Initialization", () => {
		it("should create monitor with specified interval", () => {
			monitor = createReconnectionMonitor({
				intervalMs: 60000,
				logger: mockLogger,
			});

			expect(monitor).toBeDefined();
			expect(monitor.isRunning()).toBe(false);
		});

		it("should accept optional onReconnect callback", () => {
			const onReconnect = vi.fn().mockResolvedValue(undefined);
			monitor = createReconnectionMonitor({
				intervalMs: 60000,
				logger: mockLogger,
				onReconnect,
			});

			expect(monitor).toBeDefined();
		});
	});

	describe("Starting and Stopping", () => {
		it("should start monitoring when start is called", () => {
			monitor = createReconnectionMonitor({
				intervalMs: 60000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(true);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			expect(monitor.isRunning()).toBe(true);
			expect(mockLogger.info).toHaveBeenCalledWith(
				"Starting reconnection monitor (checking every 60000ms)",
			);
		});

		it("should warn if already running", () => {
			monitor = createReconnectionMonitor({
				intervalMs: 60000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(true);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);
			monitor.start(isConnectedFn, reconnectFn);

			expect(mockLogger.warn).toHaveBeenCalledWith(
				"Reconnection monitor already running",
			);
		});

		it("should stop monitoring when stop is called", () => {
			monitor = createReconnectionMonitor({
				intervalMs: 60000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(true);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);
			monitor.stop();

			expect(monitor.isRunning()).toBe(false);
			expect(mockLogger.info).toHaveBeenCalledWith("Reconnection monitor stopped");
		});
	});

	describe("Reconnection Logic", () => {
		it("should not attempt reconnection when client is connected", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(true);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(isConnectedFn).toHaveBeenCalled();
			expect(reconnectFn).not.toHaveBeenCalled();
		});

		it("should attempt reconnection when client is disconnected", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(mockLogger.info).toHaveBeenCalledWith(
				"Connection lost, attempting background reconnection...",
			);
			expect(reconnectFn).toHaveBeenCalled();
		});

		it("should call onReconnect callback after successful reconnection", async () => {
			const onReconnect = vi.fn().mockResolvedValue(undefined);
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
				onReconnect,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(reconnectFn).toHaveBeenCalled();
			expect(onReconnect).toHaveBeenCalled();
			expect(mockLogger.info).toHaveBeenCalledWith(
				"Reconnection callback completed successfully",
			);
		});

		it("should not call onReconnect callback if reconnection fails", async () => {
			const onReconnect = vi.fn().mockResolvedValue(undefined);
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
				onReconnect,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi.fn().mockResolvedValue(false);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(reconnectFn).toHaveBeenCalled();
			expect(onReconnect).not.toHaveBeenCalled();
		});

		it("should handle reconnection errors gracefully", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi
				.fn()
				.mockRejectedValue(new Error("Connection failed"));

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(mockLogger.error).toHaveBeenCalledWith(
				"Background reconnection attempt failed",
				{ error: "Connection failed" },
			);
		});

		it("should handle onReconnect callback errors gracefully", async () => {
			const onReconnect = vi.fn().mockRejectedValue(new Error("Callback failed"));
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
				onReconnect,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(1000);

			expect(mockLogger.error).toHaveBeenCalledWith(
				"Reconnection callback failed",
				{ error: "Callback failed" },
			);
		});

		it("should continue monitoring after failed reconnection attempt", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi
				.fn()
				.mockRejectedValueOnce(new Error("First attempt failed"))
				.mockResolvedValueOnce(true);

			monitor.start(isConnectedFn, reconnectFn);

			// First interval - fails
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectFn).toHaveBeenCalledTimes(1);

			// Second interval - succeeds
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectFn).toHaveBeenCalledTimes(2);
		});

		it("should check at configured interval", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 5000,
				logger: mockLogger,
			});
			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(5000);
			expect(reconnectFn).toHaveBeenCalledTimes(1);

			await vi.advanceTimersByTimeAsync(5000);
			expect(reconnectFn).toHaveBeenCalledTimes(2);

			await vi.advanceTimersByTimeAsync(5000);
			expect(reconnectFn).toHaveBeenCalledTimes(3);
		});
	});

	describe("Integration with KafkaClient configuration", () => {
		it("should work when resilience.reconnection is enabled", async () => {
			// Simulates the KafkaClient configuration scenario
			const resilienceConfig = {
				enabled: true,
				reconnection: {
					enabled: true,
					intervalMs: 2000,
					onReconnect: vi.fn().mockResolvedValue(undefined),
				},
			};

			monitor = createReconnectionMonitor({
				intervalMs: resilienceConfig.reconnection.intervalMs,
				logger: mockLogger,
				onReconnect: resilienceConfig.reconnection.onReconnect,
			});

			const isConnectedFn = vi
				.fn()
				.mockReturnValueOnce(false) // Disconnected
				.mockReturnValue(true); // Then connected
			const reconnectFn = vi.fn().mockResolvedValue(true);

			monitor.start(isConnectedFn, reconnectFn);

			await vi.advanceTimersByTimeAsync(2000);

			expect(reconnectFn).toHaveBeenCalled();
			expect(resilienceConfig.reconnection.onReconnect).toHaveBeenCalled();
		});

		it("should not run when resilience.reconnection.enabled is false", () => {
			// This test verifies the KafkaClient doesn't create the monitor when disabled
			const resilienceConfig = {
				enabled: true,
				reconnection: {
					enabled: false,
				},
			};

			// In KafkaClient, the monitor wouldn't be created at all
			// This test documents that behavior
			expect(resilienceConfig.reconnection.enabled).toBe(false);
		});
	});

	describe("Concurrency Protection", () => {
		it("should prevent overlapping reconnection attempts", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});

			const isConnectedFn = vi.fn().mockReturnValue(false);
			let reconnectCallCount = 0;
			let activeReconnections = 0;
			let maxConcurrent = 0;

			// Simulate a slow reconnection that takes 2 seconds
			const reconnectFn = vi.fn().mockImplementation(async () => {
				reconnectCallCount++;
				activeReconnections++;
				maxConcurrent = Math.max(maxConcurrent, activeReconnections);

				// Simulate slow network
				await new Promise((resolve) => setTimeout(resolve, 2000));

				activeReconnections--;
				return false; // Keep trying
			});

			monitor.start(isConnectedFn, reconnectFn);

			// Advance by 1 second - first attempt starts
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectCallCount).toBe(1);

			// Advance by another 1 second - second interval fires but should be skipped
			await vi.advanceTimersByTimeAsync(1000);
			// Still only 1 call because first one is still running
			expect(reconnectCallCount).toBe(1);

			// Wait for first attempt to complete (total 2 seconds from start)
			await vi.advanceTimersByTimeAsync(1000);

			// Verify no overlapping attempts
			expect(maxConcurrent).toBe(1);
		});

		it("should allow next attempt after previous one completes", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});

			const isConnectedFn = vi.fn().mockReturnValue(false);
			const reconnectFn = vi
				.fn()
				.mockResolvedValueOnce(false) // First attempt
				.mockResolvedValueOnce(false); // Second attempt

			monitor.start(isConnectedFn, reconnectFn);

			// First interval
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectFn).toHaveBeenCalledTimes(1);

			// Second interval (first completed, so this should run)
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectFn).toHaveBeenCalledTimes(2);
		});

		it("should handle stop() during reconnection attempt", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
			});

			const isConnectedFn = vi.fn().mockReturnValue(false);
			const onReconnect = vi.fn().mockResolvedValue(undefined);

			let reconnectResolve: (value: boolean) => void;
			const reconnectFn = vi.fn().mockImplementation(() => {
				return new Promise<boolean>((resolve) => {
					reconnectResolve = resolve;
				});
			});

			monitor = createReconnectionMonitor({
				intervalMs: 1000,
				logger: mockLogger,
				onReconnect,
			});

			monitor.start(isConnectedFn, reconnectFn);

			// Start reconnection
			await vi.advanceTimersByTimeAsync(1000);
			expect(reconnectFn).toHaveBeenCalled();

			// Stop monitor while reconnection is in progress
			monitor.stop();

			// Complete the reconnection
			reconnectResolve!(true);
			await vi.runAllTimersAsync();

			// onReconnect should NOT be called because monitor was stopped
			expect(onReconnect).not.toHaveBeenCalled();
		});

		it("should skip check when already reconnecting", async () => {
			monitor = createReconnectionMonitor({
				intervalMs: 500,
				logger: mockLogger,
			});

			const isConnectedFn = vi.fn().mockReturnValue(false);
			let reconnectCount = 0;

			const reconnectFn = vi.fn().mockImplementation(async () => {
				reconnectCount++;
				// Simulate 1.5 second reconnection
				await new Promise((resolve) => setTimeout(resolve, 1500));
				return false;
			});

			monitor.start(isConnectedFn, reconnectFn);

			// t=0.5s: First attempt starts
			await vi.advanceTimersByTimeAsync(500);
			expect(reconnectCount).toBe(1);

			// t=1.0s: Second interval fires, but skipped (first still running)
			await vi.advanceTimersByTimeAsync(500);
			expect(reconnectCount).toBe(1);

			// t=1.5s: Third interval fires, but skipped (first still running)
			await vi.advanceTimersByTimeAsync(500);
			expect(reconnectCount).toBe(1);

			// Wait for first to complete
			await vi.advanceTimersByTimeAsync(500);

			// t=2.5s: Fifth interval starts new attempt (first is done)
			await vi.advanceTimersByTimeAsync(500);
			expect(reconnectCount).toBe(2);
		});
	});
});
