import { describe, it, expect } from 'vitest';
import { shouldTripCircuitBreaker, classifyError } from '../../../src/client/core/error-classifier';

describe('Error Classifier', () => {
  describe('shouldTripCircuitBreaker', () => {
    it('should trip on connection errors', () => {
      const error = new Error('Connection refused');
      error.name = 'KafkaJSConnectionError';
      
      expect(shouldTripCircuitBreaker(error)).toBe(true);
    });

    it('should trip on timeout errors', () => {
      const error = new Error('Request timed out');
      error.name = 'KafkaJSTimeout';
      
      expect(shouldTripCircuitBreaker(error)).toBe(true);
    });

    it('should trip on network errors', () => {
      const error = new Error('ECONNREFUSED');
      
      expect(shouldTripCircuitBreaker(error)).toBe(true);
    });

    it('should trip on broker not found errors', () => {
      const error = new Error('Broker not available');
      error.name = 'KafkaJSBrokerNotFound';
      
      expect(shouldTripCircuitBreaker(error)).toBe(true);
    });

    it('should NOT trip on authentication errors', () => {
      const error = new Error('SASL authentication failed');
      
      expect(shouldTripCircuitBreaker(error)).toBe(false);
    });

    it('should NOT trip on authorization errors', () => {
      const error = new Error('Not authorized to access topic');
      
      expect(shouldTripCircuitBreaker(error)).toBe(false);
    });

    it('should NOT trip on protocol errors', () => {
      const error = new Error('Unsupported protocol version');
      
      expect(shouldTripCircuitBreaker(error)).toBe(false);
    });

    it('should NOT trip on invalid configuration errors', () => {
      const error = new Error('Invalid configuration parameter');
      
      expect(shouldTripCircuitBreaker(error)).toBe(false);
    });

    it('should handle non-Error objects', () => {
      expect(shouldTripCircuitBreaker('string error')).toBe(false);
      expect(shouldTripCircuitBreaker(null)).toBe(false);
      expect(shouldTripCircuitBreaker(undefined)).toBe(false);
      expect(shouldTripCircuitBreaker({})).toBe(false);
    });

    it('should use retriable property if available', () => {
      const retriableError = new Error('Some error');
      (retriableError as any).retriable = true;
      
      expect(shouldTripCircuitBreaker(retriableError)).toBe(true);
      
      const nonRetriableError = new Error('Some error');
      (nonRetriableError as any).retriable = false;
      
      expect(shouldTripCircuitBreaker(nonRetriableError)).toBe(false);
    });

    it('should default to false for unknown errors', () => {
      const error = new Error('Unknown error type');
      
      expect(shouldTripCircuitBreaker(error)).toBe(false);
    });
  });

  describe('classifyError', () => {
    it('should classify connection errors', () => {
      const error = new Error('Connection failed');
      expect(classifyError(error)).toBe('connection');
    });

    it('should classify timeout errors', () => {
      const error = new Error('Request timeout');
      expect(classifyError(error)).toBe('timeout');
    });

    it('should classify authentication errors', () => {
      const error = new Error('SASL authentication failed');
      expect(classifyError(error)).toBe('authentication');
    });

    it('should classify authorization errors', () => {
      const error = new Error('Not authorized to access topic');
      expect(classifyError(error)).toBe('authorization');
    });

    it('should classify protocol errors', () => {
      const error = new Error('Protocol version mismatch');
      expect(classifyError(error)).toBe('protocol');
    });

    it('should classify unknown errors as other', () => {
      const error = new Error('Something went wrong');
      expect(classifyError(error)).toBe('other');
    });

    it('should handle non-Error objects', () => {
      expect(classifyError('string')).toBe('unknown');
      expect(classifyError(null)).toBe('unknown');
      expect(classifyError({})).toBe('unknown');
    });
  });
});
