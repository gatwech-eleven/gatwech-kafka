import { l as logger } from './kafka.Dv3r60xd.mjs';

async function connectWithRetry(connectFn, opts = {}) {
  const {
    maxAttempts = 8,
    initialDelayMs = 1e3,
    maxDelayMs = 3e4,
    factor = 2
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
    } catch (err) {
      const isDnsError = err?.code === "EAI_AGAIN" || typeof err?.message === "string" && err.message.includes("EAI_AGAIN");
      const willRetry = isDnsError || attempt < maxAttempts;
      if (!willRetry) {
        logger.error("connectWithRetry: no more retries, failing", {
          attempt,
          error: err
        });
        throw err;
      }
      const delay = Math.min(initialDelayMs * factor ** attempt, maxDelayMs);
      logger.warn("connectWithRetry: connect failed, retrying", {
        attempt,
        delay,
        retriable: isDnsError,
        error: err?.message || err
      });
      await new Promise((res) => setTimeout(res, delay));
    }
  }
}

export { connectWithRetry as c };
