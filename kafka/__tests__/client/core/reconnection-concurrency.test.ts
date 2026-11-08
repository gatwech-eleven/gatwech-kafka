import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { createReconnectionMonitor } from "../../../src/client/core/reconnection-monitor";
import type { Logger } from "../../../src/types";

describe("ReconnectionMonitor Concurrency", () => {
	const mockLogger = {
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		debug: vi.fn(),
	} as unknown as Logger;

	beforeEach(() => {
		vi.useFakeTimers();
		vi.clearAllMocks();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	it("should prevent overlapping reconnection attempts", async () => {
		const intervalMs = 100;
		const reconnectionDurationMs = 500;

		// Mock reconnect function that takes time to complete
		const reconnectFn = vi.fn().mockImplementation(async () => {
			await new Promise((resolve) => setTimeout(resolve, reconnectionDurationMs));
			return true;
		});

		// Mock isConnected to always return false (disconnected)
		const isConnectedFn = vi.fn().mockReturnValue(false);

		const monitor = createReconnectionMonitor({
			intervalMs,
			logger: mockLogger,
		});

		// Start the monitor
		monitor.start(isConnectedFn, reconnectFn);

		// Advance time by enough to trigger multiple intervals if not locked
		// We expect:
		// T=0: Interval starts (wait 100ms)
		// T=100: Tick 1 -> Start Reconnect 1 (takes 500ms)
		// T=200: Tick 2 -> Skipped (Reconnect 1 pending)
		// T=300: Tick 3 -> Skipped
		// T=400: Tick 4 -> Skipped
		// T=500: Tick 5 -> Skipped
		// T=600: Reconnect 1 finishes.
		// T=600: Tick 6 -> Start Reconnect 2

		// So in 450ms (ticks at 100, 200, 300, 400), we should only have 1 call.
		await vi.advanceTimersByTimeAsync(450);
		expect(reconnectFn).toHaveBeenCalledTimes(1);

		// Advance to completion of first attempt + next interval
		// T=600 (Reconnect 1 finishes) + 100 (next tick) = 700 total
		await vi.advanceTimersByTimeAsync(300); // 450 + 300 = 750
		expect(reconnectFn).toHaveBeenCalledTimes(2);

		monitor.stop();
	});

	it("should resume reconnection attempts after failure", async () => {
		const intervalMs = 100;

		// First attempt fails, second succeeds
		const reconnectFn = vi
			.fn()
			.mockRejectedValueOnce(new Error("Connection failed"))
			.mockResolvedValue(true);

		const isConnectedFn = vi.fn().mockReturnValue(false);

		const monitor = createReconnectionMonitor({
			intervalMs,
			logger: mockLogger,
		});

		monitor.start(isConnectedFn, reconnectFn);

		// Trigger first attempt (fails)
		await vi.advanceTimersByTimeAsync(150);
		expect(reconnectFn).toHaveBeenCalledTimes(1);
		expect(mockLogger.error).toHaveBeenCalledWith(
			expect.stringContaining("Background reconnection attempt failed"),
			expect.any(Object),
		);

		// Trigger second attempt (succeeds)
		await vi.advanceTimersByTimeAsync(100);
		expect(reconnectFn).toHaveBeenCalledTimes(2);

		monitor.stop();
	});

	it("should not reconnect if already connected", async () => {
		const intervalMs = 100;
		const reconnectFn = vi.fn().mockResolvedValue(true);
		const isConnectedFn = vi.fn().mockReturnValue(true); // Connected

		const monitor = createReconnectionMonitor({
			intervalMs,
			logger: mockLogger,
		});

		monitor.start(isConnectedFn, reconnectFn);

		await vi.advanceTimersByTimeAsync(500);
		expect(reconnectFn).not.toHaveBeenCalled();

		monitor.stop();
	});

	it("should handle stop() call during reconnection", async () => {
		const intervalMs = 100;
		const reconnectFn = vi.fn().mockImplementation(async () => {
			await new Promise((resolve) => setTimeout(resolve, 500));
			return true;
		});
		const onReconnect = vi.fn();

		const monitor = createReconnectionMonitor({
			intervalMs,
			logger: mockLogger,
			onReconnect,
		});

		monitor.start(() => false, reconnectFn);

		// Start reconnection
		await vi.advanceTimersByTimeAsync(150);
		expect(reconnectFn).toHaveBeenCalledTimes(1);

		// Stop while pending
		monitor.stop();

		// Finish reconnection
		await vi.advanceTimersByTimeAsync(500);

		// Should NOT call onReconnect because it was stopped
		expect(onReconnect).not.toHaveBeenCalled();
	});
});
