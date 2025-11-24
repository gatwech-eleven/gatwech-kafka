import express, { Request, Response } from 'express';
import { kafka, initKafka, shutdownKafka, customLogger } from './client';
import { AppTopics } from './topics';

const app = express();
app.use(express.json());

app.get('/health', (req: Request, res: Response) => {
  const detailed = req.query.detailed === 'true';
  
  if (!detailed) {
    return res.status(200).json({
      status: 'UP',
      timestamp: new Date().toISOString(),
    });
  }
  
  const kafkaHealth = kafka.getHealthStatus();
  const kafkaConnection = kafka.getConnectionStatus();
  const overallStatus = kafkaHealth.status === 'healthy' ? 'UP' : 'DEGRADED';
  
  return res.status(200).json({
    status: overallStatus,
    timestamp: new Date().toISOString(),
    services: {
      kafka: {
        status: kafkaHealth.status,
        connected: kafkaHealth.connected,
        connectionAttempts: kafkaHealth.details.connectionAttempts,
        lastError: kafkaHealth.details.lastError,
        uptime: kafkaHealth.details.uptime,
        circuitBreakerOpen: kafkaConnection.circuitBreakerOpen,
        lastConnectedAt: kafkaConnection.lastConnectedAt,
        lastDisconnectedAt: kafkaConnection.lastDisconnectedAt,
      },
    },
  });
});

app.get('/kafka/status', (req: Request, res: Response) => {
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

app.post('/kafka/reconnect', async (req: Request, res: Response) => {
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

app.post('/kafka/circuit-breaker/reset', (req: Request, res: Response) => {
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

app.post('/events/user', async (req: Request, res: Response) => {
  try {
    const { userId, event } = req.body;
    
    if (!userId || !event) {
      return res.status(400).json({
        error: 'Missing required fields: userId, event',
      });
    }
    
    if (!kafka.isReady()) {
      return res.status(503).json({
        error: 'Kafka is not connected',
        message: 'Service temporarily unavailable',
      });
    }
    
    await kafka.sendMessage(AppTopics.USER_EVENTS.name, {
      key: userId,
      value: {
        userId,
        event,
        timestamp: Date.now(),
      },
    });
    
    res.status(201).json({
      success: true,
      message: 'Event published successfully',
    });
  } catch (error) {
    customLogger.error('Failed to publish user event', error);
    res.status(500).json({
      error: 'Failed to publish event',
      message: error instanceof Error ? error.message : String(error),
    });
  }
});

app.get('/metrics', (req: Request, res: Response) => {
  const metrics = kafka.getMetrics();
  
  res.json({
    kafka: {
      publishSuccess: metrics.publishSuccess,
      publishFailures: metrics.publishFailures,
      connectionRetries: metrics.connectionRetries,
      consumerConnections: metrics.consumerConnections,
    },
  });
});

const PORT = process.env.PORT || 3000;

async function startServer() {
  await initKafka();
  
  app.listen(PORT, () => {
    customLogger.info(`🚀 Express server running on port ${PORT}`);
    customLogger.info(`📊 Health check: http://localhost:${PORT}/health?detailed=true`);
    customLogger.info(`📈 Metrics: http://localhost:${PORT}/metrics`);
    customLogger.info(`🔌 Kafka status: http://localhost:${PORT}/kafka/status`);
  });
}

async function gracefulShutdown(signal: string) {
  customLogger.info(`${signal} received, starting graceful shutdown...`);
  
  try {
    await shutdownKafka();
    process.exit(0);
  } catch (error) {
    customLogger.error('Error during shutdown', error);
    process.exit(1);
  }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

startServer().catch((error) => {
  customLogger.error('Failed to start server', error);
  process.exit(1);
});
