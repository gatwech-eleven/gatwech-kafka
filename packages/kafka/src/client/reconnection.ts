/**
 * Checks the connection every `intervalMs` and reconnects when it is down
 * @description A check or a reconnection that is still running when the next
 * tick comes is not started again. The timer doesn't keep the process alive
 */
export function createReconnectionMonitor(options: {
	/**
	 * How often to check the connection, in milliseconds
	 */
	intervalMs: number;
	/**
	 * Whether the connection works. The monitor reconnects when it doesn't
	 */
	check: () => Promise<boolean>;
	reconnect: () => Promise<void>;
	/**
	 * Called with what a check or a reconnection threw. The monitor goes on
	 */
	onError: (error: unknown) => void;
}) {
	let timer: NodeJS.Timeout | undefined;
	let isTickRunning = false;

	return {
		start: () => {
			if (timer) {
				return;
			}
			timer = setInterval(async () => {
				if (isTickRunning) {
					return;
				}
				isTickRunning = true;
				try {
					if (!(await options.check())) {
						await options.reconnect();
					}
				} catch (error) {
					options.onError(error);
				} finally {
					isTickRunning = false;
				}
			}, options.intervalMs);
			timer.unref();
		},
		stop: () => {
			if (timer) {
				clearInterval(timer);
				timer = undefined;
			}
		},
		isRunning: () => timer !== undefined,
	};
}
