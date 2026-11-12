import { KAFKA_ERROR_CODES, KafkaError } from "../error-codes";

export function createCircuitBreaker(options: {
	/**
	 * Whether the circuit ever opens. Off, `run` only calls the function
	 */
	enabled: boolean;
	/**
	 * How many calls in a row fail before the circuit opens
	 */
	failureThreshold: number;
	/**
	 * How long the circuit stays open before one call is let through, in
	 * milliseconds
	 */
	resetTimeoutMs: number;
	/**
	 * The clock, for tests
	 */
	now?: (() => number) | undefined;
}) {
	const now = options.now ?? Date.now;
	let failures = 0;
	let openedAt: number | undefined;
	let isTrialRunning = false;

	const state = (): "closed" | "open" | "half-open" => {
		if (!options.enabled || openedAt === undefined) {
			return "closed";
		}
		return isTrialRunning || now() - openedAt >= options.resetTimeoutMs
			? "half-open"
			: "open";
	};

	return {
		state,
		/**
		 * Calls the function unless the circuit is open
		 * @description When it is, throws `CIRCUIT_OPEN` without calling it. Once
		 * `resetTimeoutMs` has passed, exactly one call is let through: if it works
		 * the circuit closes, and if it fails the circuit opens again. The calls
		 * that arrive while it runs are refused
		 */
		run: async <T>(fn: () => Promise<T>): Promise<T> => {
			if (!options.enabled) {
				return fn();
			}

			const circuitState = state();
			if (
				circuitState === "open" ||
				(circuitState === "half-open" && isTrialRunning)
			) {
				throw new KafkaError(KAFKA_ERROR_CODES.CIRCUIT_OPEN);
			}
			const isTrial = circuitState === "half-open";
			if (isTrial) {
				isTrialRunning = true;
			}

			try {
				const returned = await fn();
				failures = 0;
				openedAt = undefined;
				return returned;
			} catch (error) {
				failures++;
				// The count stays at the threshold while the circuit is open, so a
				// failed trial opens it again
				if (failures >= options.failureThreshold) {
					openedAt = now();
				}
				throw error;
			} finally {
				if (isTrial) {
					isTrialRunning = false;
				}
			}
		},
	};
}
