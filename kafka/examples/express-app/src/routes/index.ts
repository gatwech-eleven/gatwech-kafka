import { Router } from 'express';
import healthRoutes from './health';
import kafkaRoutes from './kafka';
import eventsRoutes from './events';
import metricsRoutes from './metrics';

const router = Router();

// Mount routes
router.use('/health', healthRoutes);
router.use('/ready', healthRoutes); // Alias for K8s
router.use('/live', healthRoutes);  // Alias for K8s
router.use('/kafka', kafkaRoutes);
router.use('/events', eventsRoutes);
router.use('/metrics', metricsRoutes);

export default router;
