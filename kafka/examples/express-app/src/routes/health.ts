import { Router, Request, Response } from "express";
import { kafka } from "../client";

const router = Router();

// Detailed health check
router.get("/", (req: Request, res: Response) => {
	const detailed = req.query.detailed === "true";

	if (!detailed) {
		return res.status(200).json({
			status: "UP",
			timestamp: new Date().toISOString(),
		});
	}

	const kafkaHealth = kafka.getHealthStatus();
	const kafkaConnection = kafka.getConnectionStatus();
	const overallStatus = kafkaHealth.status === "healthy" ? "UP" : "DEGRADED";

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

// Kubernetes readiness probe
router.get("/ready", (req: Request, res: Response) => {
	const health = kafka.getHealthStatus();
	const ready = health.status !== "unhealthy";

	res.status(ready ? 200 : 503).json({
		ready,
		kafka: health.status,
		timestamp: new Date().toISOString(),
	});
});

// Kubernetes liveness probe
router.get("/live", (req: Request, res: Response) => {
	const health = kafka.getHealthStatus();
	const alive = health.status !== "unhealthy";

	res.status(alive ? 200 : 503).json({
		alive,
		timestamp: new Date().toISOString(),
	});
});

export default router;
