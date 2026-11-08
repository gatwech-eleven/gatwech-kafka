import type { Logger } from "../../types";

export interface ReconnectionMonitor {
	/** Starts the monitoring loop. */
	start: (
		isConnectedFn: () => boolean,
		reconnectFn: () => Promise<boolean>,
	) => void;
	/** Stops the monitoring loop immediately. */
	stop: () => void;
	/** Returns true if the monitor is currently active. */
	isRunning: () => boolean;
}

export interface ReconnectionMonitorOptions {
	/** How often to check connection status in milliseconds. */
	intervalMs: number;
	/** Logger instance. */
	logger: Logger;
	/** Optional callback to run after a successful reconnection. */
	onReconnect?: () => Promise<void>;
}

/**
 * Creates a reconnection monitor that periodically checks connection status
 * and attempts reconnection when disconnected.
 *
 * **Concurrency Note:** This implementation prevents overlapping reconnection
 * attempts. If a reconnection takes longer than `intervalMs`, the next check
 * is skipped until the current attempt finishes.
 *
 * @example
 * ```typescript
 * const monitor = createReconnectionMonitor({
 *   intervalMs: 5000,
 *   logger,
 *   onReconnect: async () => {
 *     await consumer.subscribe({ topic: 'logs' });
 *   }
 * });
 *
 * monitor.start(
 *   () => client.isConnected(),
 *   () => client.reconnect()
 * );
 * ```
 */
export function createReconnectionMonitor(
	options: ReconnectionMonitorOptions,
): ReconnectionMonitor {
	const { intervalMs, logger, onReconnect } = options;

	let interval: NodeJS.Timeout | null = null;
	let isReconnecting = false; // Concurrency guard flag

	const start = (
		isConnectedFn: () => boolean,
		reconnectFn: () => Promise<boolean>,
	): void => {
		if (interval) {
			logger.warn("Reconnection monitor already running");
			return;
		}

		logger.info(`Starting reconnection monitor (checking every ${intervalMs}ms)`);

		interval = setInterval(async () => {
			// 1. If we are already trying to reconnect, skip this tick.
			if (isReconnecting) return;

			// 2. If we are connected, do nothing.
			if (isConnectedFn()) return;

			// 3. Begin Reconnection Logic
			isReconnecting = true;
			logger.info("Connection lost, attempting background reconnection...");

			try {
				const success = await reconnectFn();

				// 4. Safety Check: Did the user call stop() while we were awaiting?
				if (!interval) {
					isReconnecting = false;
					return;
				}

				if (success && onReconnect) {
					try {
						await onReconnect();
						logger.info("Reconnection callback completed successfully");
					} catch (error) {
						logger.error("Reconnection callback failed", {
							error: error instanceof Error ? error.message : String(error),
						});
					}
				}
			} catch (error) {
				logger.error("Background reconnection attempt failed", {
					error: error instanceof Error ? error.message : String(error),
				});
			} finally {
				// 5. Always release the lock
				isReconnecting = false;
			}
		}, intervalMs);
	};

	const stop = (): void => {
		if (interval) {
			clearInterval(interval);
			interval = null;
			isReconnecting = false; // Reset state
			logger.info("Reconnection monitor stopped");
		}
	};

	const isRunning = (): boolean => {
		return interval !== null;
	};

	return {
		start,
		stop,
		isRunning,
	};
}
