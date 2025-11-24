import type { Logger } from "../../types";

/**
 * Background monitor that periodically checks connection status
 * and attempts reconnection when disconnected.
 * 
 * @example
 * ```typescript
 * const monitor = new ReconnectionMonitor(
 *   60000, // Check every 60s
 *   logger,
 *   async () => {
 *     console.log('Reconnected! Re-initializing...');
 *   }
 * );
 * 
 * monitor.start(
 *   () => client.isConnected(),
 *   () => client.reconnect()
 * );
 * 
 * // Later, on shutdown
 * monitor.stop();
 * ```
 */
export class ReconnectionMonitor {
  private interval: NodeJS.Timeout | null = null;
  private intervalMs: number;
  private logger: Logger;
  private onReconnect?: () => Promise<void>;

  constructor(
    intervalMs: number,
    logger: Logger,
    onReconnect?: () => Promise<void>
  ) {
    this.intervalMs = intervalMs;
    this.logger = logger;
    this.onReconnect = onReconnect;
  }

  /**
   * Start the reconnection monitor.
   * 
   * @param isConnectedFn - Function that returns current connection status
   * @param reconnectFn - Function that attempts reconnection
   */
  start(
    isConnectedFn: () => boolean,
    reconnectFn: () => Promise<boolean>
  ): void {
    if (this.interval) {
      this.logger.warn("Reconnection monitor already running");
      return;
    }

    this.logger.info(
      `Starting reconnection monitor (checking every ${this.intervalMs}ms)`
    );

    this.interval = setInterval(async () => {
      if (!isConnectedFn()) {
        this.logger.info("Connection lost, attempting background reconnection...");
        
        try {
          const success = await reconnectFn();

          if (success && this.onReconnect) {
            try {
              await this.onReconnect();
              this.logger.info("Reconnection callback completed successfully");
            } catch (error) {
              this.logger.error("Reconnection callback failed", {
                error: error instanceof Error ? error.message : String(error),
              });
            }
          }
        } catch (error) {
          this.logger.error("Background reconnection attempt failed", {
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }, this.intervalMs);
  }

  /**
   * Stop the reconnection monitor.
   */
  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
      this.logger.info("Reconnection monitor stopped");
    }
  }

  /**
   * Check if monitor is currently running.
   */
  isRunning(): boolean {
    return this.interval !== null;
  }
}
