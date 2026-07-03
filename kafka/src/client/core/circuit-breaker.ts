export interface CircuitBreaker {
  recordFailure: () => void;
  recordSuccess: () => void;
  isOpen: () => boolean;
  isCircuitOpen: () => boolean;
  reset: () => void;
  getFailureCount: () => number;
}

export interface CircuitBreakerOptions {
  threshold: number;
  resetTimeoutMs?: number;
}

/**
 * Creates a circuit breaker with "Half-Open" recovery logic.
 * 
 * Prevents cascading failures by opening the circuit after a threshold
 * of consecutive failures. After a configurable timeout, the circuit
 * enters a "Half-Open" state, allowing a test request to determine
 * if the service has recovered.
 * 
 * **States:**
 * - **Closed**: Normal operation, requests allowed
 * - **Open**: Too many failures, requests blocked
 * - **Half-Open**: Timeout passed, allowing one test request
 * 
 * @example
 * ```typescript
 * // Trips after 5 failures, waits 10 seconds before allowing a retry
 * const breaker = createCircuitBreaker({ threshold: 5, resetTimeoutMs: 10000 });
 * 
 * if (breaker.isOpen()) {
 *   throw new Error('Circuit open - Kafka unavailable');
 * }
 * 
 * try {
 *   await connect();
 *   breaker.recordSuccess();
 * } catch (err) {
 *   breaker.recordFailure();
 *   throw err;
 * }
 * ```
 */
export function createCircuitBreaker(
  options: CircuitBreakerOptions
): CircuitBreaker {
  const { threshold, resetTimeoutMs = 30000 } = options;
  
  let failureCount = 0;
  let circuitOpen = false;
  let lastFailureTime = 0;

  const recordFailure = (): void => {
    failureCount++;
    lastFailureTime = Date.now();

    if (failureCount >= threshold) {
      circuitOpen = true;
    }
  };

  const recordSuccess = (): void => {
    failureCount = 0;
    circuitOpen = false;
    lastFailureTime = 0;
  };

  const isOpen = (): boolean => {
    if (circuitOpen) {
      const now = Date.now();
      const timeSinceFailure = now - lastFailureTime;

      // If timeout passed, we allow this request (Half-Open state)
      if (timeSinceFailure > resetTimeoutMs) {
        return false; 
      }
      
      return true; // Still in cooldown
    }
    
    return false; // Normal operation (Closed)
  };

  const isCircuitOpen = (): boolean => {
    return isOpen();
  };

  const reset = (): void => {
    recordSuccess();
  };

  const getFailureCount = (): number => {
    return failureCount;
  };

  return {
    recordFailure,
    recordSuccess,
    isOpen,
    isCircuitOpen,
    reset,
    getFailureCount,
  };
}
