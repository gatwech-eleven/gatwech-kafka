import { KafkaJSError } from "kafkajs";

/**
 * Determines if an error should trip the circuit breaker.
 * 
 * **Should trip (retriable connection errors):**
 * - Connection errors (broker unavailable)
 * - Timeout errors
 * - Network errors
 * 
 * **Should NOT trip (non-retriable errors):**
 * - Protocol errors (wrong API version)
 * - Authentication/authorization errors
 * - Invalid configuration errors
 * - Application logic errors
 * 
 * @param error - The error to classify
 * @returns true if the error should trip the circuit breaker
 * 
 * @example
 * ```typescript
 * try {
 *   await kafka.connect();
 * } catch (error) {
 *   if (shouldTripCircuitBreaker(error)) {
 *     circuitBreaker.recordFailure();
 *   }
 *   throw error;
 * }
 * ```
 */
export function shouldTripCircuitBreaker(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const errorMessage = error.message.toLowerCase();
  const errorName = error.name.toLowerCase();

  // KafkaJS-specific error types that should trip the breaker
  const retriableErrors = [
    'kafkajsconnectionerror',
    'kafkajstimeout',
    'kafkajsnetworkerror',
    'kafkajsbrokernotfound',
  ];

  if (retriableErrors.some(name => errorName.includes(name))) {
    return true;
  }

  // Check for connection-related error messages
  const connectionErrorPatterns = [
    'econnrefused',
    'enotfound',
    'etimedout',
    'econnreset',
    'epipe',
    'broker',
    'connection',
    'timeout',
    'network',
  ];

  if (connectionErrorPatterns.some(pattern => errorMessage.includes(pattern))) {
    return true;
  }

  // Non-retriable errors that should NOT trip the breaker
  const nonRetriablePatterns = [
    'authentication',
    'authorization',
    'sasl',
    'invalid',
    'unsupported',
    'protocol',
    'version',
  ];

  if (nonRetriablePatterns.some(pattern => errorMessage.includes(pattern))) {
    return false;
  }

  // If error is a KafkaJSError and has retriable property, use it
  if ('retriable' in error && typeof (error as any).retriable === 'boolean') {
    return (error as any).retriable;
  }

  // Default: don't trip the breaker for unknown errors
  return false;
}

/**
 * Classifies an error into a category for logging and metrics.
 * 
 * @param error - The error to classify
 * @returns Error category string
 */
export function classifyError(error: unknown): string {
  if (!(error instanceof Error)) {
    return 'unknown';
  }

  const errorName = error.name.toLowerCase();
  const errorMessage = error.message.toLowerCase();

  if (errorName.includes('connection') || errorMessage.includes('connection')) {
    return 'connection';
  }

  if (errorName.includes('timeout') || errorMessage.includes('timeout')) {
    return 'timeout';
  }

  if (errorMessage.includes('authentication') || errorMessage.includes('sasl')) {
    return 'authentication';
  }

  if (errorName.includes('author') || errorMessage.includes('author')) {
    return 'authorization';
  }

  if (errorMessage.includes('protocol') || errorMessage.includes('version')) {
    return 'protocol';
  }

  return 'other';
}
