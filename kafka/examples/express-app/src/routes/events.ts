import { Router, Request, Response } from "express";
import { kafka, customLogger } from "../client";
import { AppTopics } from "../topics";

const router = Router();

// Publish user event
router.post("/user", async (req: Request, res: Response) => {
	try {
		const { userId, event } = req.body;

		if (!userId || !event) {
			return res.status(400).json({
				error: "Missing required fields: userId, event",
			});
		}

		if (!kafka.isReady()) {
			return res.status(503).json({
				error: "Kafka is not connected",
				message: "Service temporarily unavailable",
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
			message: "Event published successfully",
		});
	} catch (error) {
		customLogger.error("Failed to publish user event", { error });
		res.status(500).json({
			error: "Failed to publish event",
			message: error instanceof Error ? error.message : String(error),
		});
	}
});

// Publish order event
router.post("/order", async (req: Request, res: Response) => {
	try {
		const { orderId, userId, amount, status } = req.body;

		if (!orderId || !userId || amount === undefined || !status) {
			return res.status(400).json({
				error: "Missing required fields: orderId, userId, amount, status",
			});
		}

		if (!kafka.isReady()) {
			return res.status(503).json({
				error: "Kafka is not connected",
				message: "Service temporarily unavailable",
			});
		}

		await kafka.sendMessage(AppTopics.ORDER_EVENTS.name, {
			key: orderId,
			value: {
				orderId,
				userId,
				amount,
				status,
				timestamp: Date.now(),
			},
		});

		res.status(201).json({
			success: true,
			message: "Order event published successfully",
		});
	} catch (error) {
		customLogger.error("Failed to publish order event", { error });
		res.status(500).json({
			error: "Failed to publish event",
			message: error instanceof Error ? error.message : String(error),
		});
	}
});

export default router;
