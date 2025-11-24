/**
 * Example: Modular Kafka Client with Express
 *
 * This example demonstrates how to integrate the modular Kafka client
 * into a standalone Express application.
 */

import express from "express";
import {
  createKafkaClient,
  topicProvisioningExtension,
  createTopicConfig,
  type GatwechKafkaExtensions,
} from "../src";
import { logger } from "../src/utils/logger";
import { z } from "@gatwech/utils/zod";

const DEFAULT_PORT = 3000;
const LOG_LEVEL_INFO = 4;

// Define your topics
const TopicSpecifications = createTopicConfig({
  USER_CREATED: {
    name: "user.created",
    schema: z.object({ id: z.string(), email: z.string() }),
    retentionMs: 7 * 24 * 60 * 60 * 1000,
  },
  ORDER_PROCESSED: {
    name: "order.processed",
    schema: z.object({ orderId: z.string(), amount: z.number() }),
    cleanupPolicy: "compact",
  },
});

// Define extensions with proper typing
const extensions = [
  topicProvisioningExtension(TopicSpecifications, {
    partitions: 3,
    replicationFactor: 1,
  }),
] satisfies GatwechKafkaExtensions;

// Initialize the Kafka client
const kafka = createKafkaClient({
  config: {
    clientId: "express-service",
    brokers: [process.env.KAFKA_BOOTSTRAP || "localhost:9092"],
    metricsEnabled: true,
    logLevel: LOG_LEVEL_INFO,
  },
  extensions,
});

// Setup Express App
const app = express();
app.use(express.json());

// Endpoint to produce a message
app.post("/users", async (req, res) => {
  try {
    const { id, email } = req.body;
    
    // Validate payload against topic schema
    const payload = TopicSpecifications.USER_CREATED.schema.parse({ id, email });

    await kafka.sendMessage(
      TopicSpecifications.USER_CREATED.name,
      {
        key: id,
        value: payload,
      }
    );

    res.status(201).json({ message: "User created event published" });
  } catch (error) {
    logger.error("Failed to publish message:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// Health check endpoint using built-in method
app.get("/health", async (req, res) => {
  const isHealthy = await kafka.healthCheck();
  if (isHealthy) {
    res.status(200).json({ status: "ok", kafka: "connected" });
  } else {
    res.status(503).json({ status: "error", kafka: "disconnected" });
  }
});

// Start server
const PORT = process.env.PORT || DEFAULT_PORT;

async function start() {
  try {
    // Connect to Kafka
    await kafka.connect();
    logger.info("✅ Kafka connected");

    // Provision topics
    await kafka.extensions.topicProvisioning.provisionTopics();
    logger.info("✅ Topics provisioned");

    // Start Express
    const server = app.listen(PORT, () => {
      logger.info(`🚀 Server running on http://localhost:${PORT}`);
    });

    // Graceful shutdown
    const shutdown = async () => {
      logger.info("Shutting down...");
      server.close();
      await kafka.disconnect();
      process.exit(0);
    };

    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);

  } catch (error) {
    logger.error("Failed to start application:", error);
    process.exit(1);
  }
}

if (require.main === module) {
  start();
}
