import { Router, Request, Response } from 'express';
import { kafka, customLogger } from '../client';

const router = Router();

// Kafka connection status
router.get('/status', (req: Request, res: Response) => {
  const status = kafka.getConnectionStatus();
  
  res.json({
    connected: status.connected,
    connectionAttempts: status.connectionAttempts,
    lastError: status.lastError,
    lastConnectedAt: status.lastConnectedAt,
    lastDisconnectedAt: status.lastDisconnectedAt,
    circuitBreakerOpen: status.circuitBreakerOpen,
  });
});

// Manual reconnection
router.post('/reconnect', async (req: Request, res: Response) => {
  try {
    customLogger.info('Manual reconnection triggered via API');
    const success = await kafka.reconnect();
    
    if (success) {
      res.json({
        success: true,
        message: 'Reconnected successfully',
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Reconnection failed',
      });
    }
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Reconnection error',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// Reset circuit breaker
router.post('/circuit-breaker/reset', (req: Request, res: Response) => {
  try {
    kafka.resetCircuitBreaker();
    customLogger.info('Circuit breaker reset via API');
    
    res.json({
      success: true,
      message: 'Circuit breaker reset successfully',
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to reset circuit breaker',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// Resilience features status
router.get('/resilience', (req: Request, res: Response) => {
  const connectionStatus = kafka.getConnectionStatus();
  const healthStatus = kafka.getHealthStatus();
  
  res.json({
    resilience: {
      circuitBreaker: {
        state: connectionStatus.circuitBreakerOpen ? 'OPEN' : 'CLOSED',
        description: connectionStatus.circuitBreakerOpen
          ? 'Circuit is OPEN - blocking requests (will test after timeout)'
          : 'Circuit is CLOSED - allowing requests',
        failureThreshold: 5,
        resetTimeoutMs: 30000,
        note: 'After 5 failures, circuit opens. After 30s, enters Half-Open state to test recovery.',
      },
      reconnectionMonitor: {
        active: true,
        intervalMs: 60000,
        description: 'Background monitor checks connection every 60s',
        concurrencySafe: true,
        note: 'Prevents overlapping reconnection attempts - if one is running, next check is skipped.',
      },
      connection: {
        status: healthStatus.status,
        connected: connectionStatus.connected,
        attempts: connectionStatus.connectionAttempts,
        lastError: connectionStatus.lastError,
        lastConnectedAt: connectionStatus.lastConnectedAt,
        lastDisconnectedAt: connectionStatus.lastDisconnectedAt,
        uptime: healthStatus.details.uptime,
      },
    },
    demo: {
      endpoints: {
        resetCircuitBreaker: 'POST /kafka/circuit-breaker/reset',
        manualReconnect: 'POST /kafka/reconnect',
        publishEvent: 'POST /events/user',
      },
      howToTest: [
        '1. Stop Kafka broker to trigger failures',
        '2. Try publishing events - circuit will open after 5 failures',
        '3. Start Kafka broker again',
        '4. Wait 30s for Half-Open state, or manually reset circuit',
        '5. Background monitor will auto-reconnect within 60s',
      ],
    },
  });
});

export default router;
