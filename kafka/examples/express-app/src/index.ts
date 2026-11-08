import express, { Request, Response } from "express";
import { kafka, initKafka, shutdownKafka, customLogger } from "./client";
import routes from "./routes";

const app = express();
app.use(express.json());

// Simple health check for the demo app
app.get("/api/v1/express-demo-app/health", (_req: Request, res: Response) => {
	return res.status(200).json({
		status: "UP",
		message: "Express Kafka Demo App is running",
		timestamp: new Date().toISOString(),
	});
});

// Mount all other routes
app.use(routes);

const PORT = process.env.PORT || 5757;

/**
 * Initializes Kafka and starts the HTTP server.
 * Uses graceful degradation - app starts even if Kafka is down.
 */
async function startServer() {
	try {
		customLogger.info("Initializing Kafka client...");

		// ✅ Initialize Kafka (non-blocking with autoConnectAsync)
		const kafkaInitialized = await initKafka();

		if (kafkaInitialized) {
			customLogger.info("Kafka: initialized");
		} else {
			customLogger.warn("Kafka: initialization failed - running in degraded mode");
		}

		customLogger.info("Core services initialization complete.");

		// Start HTTP server
		app.listen(PORT, () => {
			customLogger.info(`🚀 Express server running on port ${PORT}`);

			// Log Kafka status after startup
			const kafkaStatus = kafka.isReady() ? "Connected" : "Connecting";
			customLogger.info(`Kafka: ${kafkaStatus}`);

			if (kafka.isReady()) {
				import("./consumers")
					.then(async ({ initializeConsumers }) => {
						await initializeConsumers();
						customLogger.info("Consumers initialized successfully");
					})
					.catch((error) => {
						customLogger.error("Failed to initialize consumers", { error });
					});
			} else {
				customLogger.info("Kafka not ready - consumers will start on reconnect");
			}
		});
	} catch (err) {
		customLogger.error("FATAL: Application bootstrap failed.", { error: err });
		process.exit(1);
	}
}

async function gracefulShutdown(signal: string) {
	customLogger.info(`${signal} received, starting graceful shutdown...`);

	try {
		// Shutdown consumers first
		try {
			const { shutdownConsumers } = await import("./consumers");
			await shutdownConsumers();
			customLogger.info("Consumers shut down");
		} catch (error) {
			customLogger.warn(
				"Failed to shutdown consumers (they may not be initialized)",
				{ error },
			);
		}

		// Then shutdown Kafka client
		await shutdownKafka();
		customLogger.info("Kafka disconnected");

		process.exit(0);
	} catch (error) {
		customLogger.error("Error during shutdown", { error });
		process.exit(1);
	}
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));

startServer().catch((error) => {
	customLogger.error("Failed to start server", { error });
	process.exit(1);
});
