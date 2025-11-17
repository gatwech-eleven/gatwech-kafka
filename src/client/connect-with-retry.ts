import { logger } from "../utils/logger";
import type { DeepReadonly } from "../types/utility-types";

export interface ConnectRetryOptions {
  maxAttempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
}

/**
 * Generic helper to retry an async connect function with exponential backoff.
 * Detects transient DNS errors (EAI_AGAIN) and will keep retrying up to maxAttempts.
 */
export async function connectWithRetry(
  connectFn: () => Promise<void>,
  opts: DeepReadonly<ConnectRetryOptions> = {}
): Promise<void> {
  const {
    maxAttempts = 8,
    initialDelayMs = 1000,
    maxDelayMs = 30_000,
    factor = 2,
  } = opts;

  let attempt = 0;

  while (true) {
    try {
      attempt++;
      await connectFn();
      if (attempt > 1) {
        logger.info("connectWithRetry: connection succeeded", { attempt });
      }
      return;
    } catch (err: any) {
      const isDnsError =
        err?.code === "EAI_AGAIN" ||
        (typeof err?.message === "string" && err.message.includes("EAI_AGAIN"));

      const willRetry = isDnsError || attempt < maxAttempts;

      if (!willRetry) {
        logger.error("connectWithRetry: no more retries, failing", {
          attempt,
          error: err,
        });
        throw err;
      }

      const delay = Math.min(initialDelayMs * factor ** attempt, maxDelayMs);

      logger.warn("connectWithRetry: connect failed, retrying", {
        attempt,
        delay,
        retriable: isDnsError,
        error: err?.message || err,
      });

      await new Promise((res) => setTimeout(res, delay));
      // loop to retry
    }
  }
}

export default connectWithRetry;
