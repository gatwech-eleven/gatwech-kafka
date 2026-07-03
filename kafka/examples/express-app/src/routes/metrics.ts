import { Router, Request, Response } from 'express';
import { kafka } from '../client';

const router = Router();

// Kafka metrics
router.get('/', (req: Request, res: Response) => {
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

export default router;
