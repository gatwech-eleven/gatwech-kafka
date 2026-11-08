import { nanoid } from "nanoid";
import supertest from "supertest";
import { z } from "@gatwech/utils/zod";
import express, { type Request, type Response } from "express";
import type { EachMessagePayload } from "kafkajs";
import { createKafkaClient } from "../../src/client/core/create-client.ts";
import { createTopicConfig } from "../../src/config/topics/define-topics.ts";
import { createServicePublisherFactory } from "../../src/client/producers/service-publisher-factory.ts";
import { UserRegisteredSchema } from "../../src/schemas/user-schemas.ts";
import { consumeWithRetryAndDLQ } from "../../src/client/consumers/consume-with-dlq.ts";

async function sleep(ms: number) {
	return new Promise((r) => setTimeout(r, ms));
}

async function main() {
	const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
	const topic = process.env.EXPRESS_INTEGRATION_TOPIC || "user.registered";
	const authSource = process.env.AUTH_SOURCE || "auth-express-e2e";

	console.log(`Express Integration: bootstrap=${bootstrap}, topic=${topic}`);

	// Define topics using createTopicConfig
	const TestTopics = createTopicConfig({
		USER_REGISTERED: {
			name: topic,
			schema: UserRegisteredSchema,
			retentionMs: 86400000,
			cleanupPolicy: "delete",
		},
		USER_REGISTERED_DLQ: {
			name: `${topic}.dlq`,
			schema: z.any(), // DLQ can contain any failed message
			retentionMs: 86400000,
			cleanupPolicy: "delete",
		},
	});

	// Shared: ensure topic exists using modular client
	const adminClient = createKafkaClient({
		config: {
			clientId: "admin-express-e2e",
			brokers: [bootstrap],
			metricsEnabled: false,
			connectWithRetries: true,
			topics: TestTopics,
			topicProvisioning: {
				partitions: 3,
				replicationFactor: 1,
				waitForLeadersMs: 15000,
			},
		},
	});

	await adminClient.connect();
	try {
		await adminClient.provisionTopics();
	} finally {
		await adminClient.disconnect();
	}

	// Notification service (consumer) + app
	const notificationClient = createKafkaClient({
		config: {
			clientId: "notification-express-e2e",
			brokers: [bootstrap],
			metricsEnabled: false,
			connectWithRetries: true,
		},
	});

	await notificationClient.connect();
	const groupId = `notification-express-${nanoid().slice(0, 6)}`;
	let lastProcessed: any = null;
	let lastDlq: any = null;

	const topicConfig = {
		name: topic,
		retentionMs: 86400000,
		cleanupPolicy: "delete" as const,
		schema: UserRegisteredSchema,
	};

	// Main consumer with retry + DLQ
	const consumer = await consumeWithRetryAndDLQ(
		notificationClient,
		groupId,
		topicConfig,
		async (payload: z.infer<typeof UserRegisteredSchema>) => {
			// Simulate failure for DLQ when requested
			if (
				payload?.name === "FAIL" ||
				String(payload?.email).startsWith("express-dlq-")
			) {
				throw new Error("simulated failure for DLQ");
			}
			lastProcessed = payload;
		},
		{ dlqEnabled: true, maxRetries: 2, retryBackoffMs: 200 },
	);

	// DLQ consumer to assert DLQ publication
	const dlqGroupId = `${groupId}-dlq`;
	const dlqConsumer = notificationClient.createConsumer({
		groupId: dlqGroupId,
	});
	await dlqConsumer.connect();
	await dlqConsumer.subscribe({ topic: `${topic}.dlq`, fromBeginning: true });
	dlqConsumer
		.run({
			eachMessage: async ({ message }: EachMessagePayload) => {
				try {
					const raw = message.value?.toString() || "{}";
					lastDlq = JSON.parse(raw);
				} catch {}
			},
		})
		.catch((e: Error) => console.error("DLQ consumer error", e));

	const notificationApp = express();
	notificationApp.get("/health", (_req: Request, res: Response) =>
		res.json({ ok: true }),
	);
	notificationApp.get("/events/last", (_req: Request, res: Response) =>
		res.json({ lastProcessed }),
	);

	const authClient = createKafkaClient({
		config: {
			clientId: "auth-express-e2e",
			brokers: [bootstrap],
			metricsEnabled: false,
			connectWithRetries: true,
		},
	});

	await authClient.connect();
	const producer = authClient.getProducer();
	const defineEvent = createServicePublisherFactory(producer, authSource);
	const getPartitionKey = (p: z.infer<typeof UserRegisteredSchema>) => p.email;
	const publishUserRegistered = defineEvent(
		topic,
		UserRegisteredSchema,
		getPartitionKey,
	);

	const authApp = express();
	authApp.use(express.json());
	authApp.post("/signup", async (req: Request, res: Response) => {
		try {
			const body = (req.body as z.infer<typeof UserRegisteredSchema>) || {};
			// Validate and publish using service publisher
			await publishUserRegistered({
				id: body.id || nanoid(),
				email: body.email,
				name: body.name,
			} as z.infer<typeof UserRegisteredSchema>);
			res.status(202).json({ status: "accepted" });
		} catch (e: any) {
			res.status(400).json({ error: e?.message || String(e) });
		}
	});

	// Use supertest to call the auth app without binding ports
	const authRequest = supertest(authApp);
	const notificationRequest = supertest(notificationApp);

	const email = `express-${nanoid().slice(0, 4)}@example.com`;
	console.log("POST /signup", { email });
	const resp = await authRequest
		.post("/signup")
		.send({ email, name: "Express User" });
	if (resp.status !== 202)
		throw new Error(`Signup failed: ${resp.status} ${resp.text}`);

	// Poll the notification app to confirm processing
	const start = Date.now();
	const timeoutMs = 20000;
	while (Date.now() - start < timeoutMs) {
		const got = await notificationRequest.get("/events/last");
		if (got.body?.lastProcessed?.email === email) {
			console.log("Express Integration Success:", got.body.lastProcessed);
			break;
		}
		await sleep(500);
	}
	if (!lastProcessed || lastProcessed.email !== email) {
		throw new Error("Timeout waiting for notification processing");
	}

	// Now send a message that should be DLQ'd
	const dlqEmail = `express-dlq-${nanoid().slice(0, 4)}@example.com`;
	console.log("POST /signup (DLQ)", { email: dlqEmail });
	const resp2 = await authRequest
		.post("/signup")
		.send({ email: dlqEmail, name: "FAIL" });
	if (resp2.status !== 202)
		throw new Error(`Signup (DLQ) failed: ${resp2.status} ${resp2.text}`);

	const dlqStart = Date.now();
	while (Date.now() - dlqStart < timeoutMs) {
		if (lastDlq && lastDlq.originalValue?.email === dlqEmail) {
			console.log("🧰 DLQ observed:", lastDlq);
			break;
		}
		await sleep(500);
	}
	if (!lastDlq || lastDlq.originalValue?.email !== dlqEmail) {
		throw new Error("Timeout waiting for DLQ message");
	}

	// Cleanup
	await consumer.disconnect();
	await dlqConsumer.disconnect();
	await notificationClient.disconnect();
	await authClient.disconnect();
}

main().catch((err) => {
	console.error("Express Integration Failed:", err);
	process.exit(1);
});
