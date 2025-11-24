/**
 * Circuit breaker to prevent cascading failures.
 * 
 * Opens the circuit after a threshold of consecutive failures,
 * preventing further connection attempts until manually reset or
 * a successful connection occurs.
 * 
 * @example
 * ```typescript
 * const breaker = new CircuitBreaker(5);
 * 
 * if (breaker.isCircuitOpen()) {
 *   throw new Error('Circuit breaker is open');
 * }
 * 
 * try {
 *   await connect();
 *   breaker.recordSuccess();
 * } catch (error) {
 *   breaker.recordFailure();
 *   throw error;
 * }
 * ```
 */
export class CircuitBreaker {
  private failureCount = 0;
  private threshold: number;
  private isOpen = false;

  constructor(threshold: number) {
    this.threshold = threshold;
  }

  /**
   * Record a connection failure.
   * Opens the circuit if failure count reaches threshold.
   */
  recordFailure(): void {
    this.failureCount++;
    if (this.failureCount >= this.threshold) {
      this.isOpen = true;
    }
  }

  /**
   * Record a successful connection.
   * Resets failure count and closes the circuit.
   */
  recordSuccess(): void {
    this.failureCount = 0;
    this.isOpen = false;
  }

  /**
   * Check if the circuit is open.
   * 
   * @returns true if circuit is open (too many failures)
   */
  isCircuitOpen(): boolean {
    return this.isOpen;
  }

  /**
   * Manually reset the circuit breaker.
   * Closes the circuit and resets failure count.
   */
  reset(): void {
    this.failureCount = 0;
    this.isOpen = false;
  }

  /**
   * Get current failure count.
   */
  getFailureCount(): number {
    return this.failureCount;
  }
}
