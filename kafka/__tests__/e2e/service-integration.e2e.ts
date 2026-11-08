import { BaseKafkaClient } from "../../src/client/core/kafka.ts";
import { createServicePublisherFactory } from "../../src/client/producers/service-publisher-factory.ts";
import { initializeKafkaTopics } from "../../src/config/initialize.topics.ts";
import { UserRegisteredSchema } from "../../src/schemas/user-schemas.ts";
import { z } from "@gatwech/utils/zod";
import { nanoid } from "nanoid";

async function main() {
	const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
	const topic = process.env.INTEGRATION_TOPIC || "user.registered";
	const authSource = process.env.AUTH_SOURCE || "auth-service";
	const notificationGroup =
		process.env.NOTIFICATION_GROUP || `notification-svc-${nanoid().slice(0, 6)}`;

	console.log(`Integration E2E: bootstrap=${bootstrap}, topic=${topic}`);

	// Two logical services using separate clients
	const authClient = new BaseKafkaClient("auth-client", [bootstrap], {
		metricsEnabled: false,
		connectWithRetries: true,
		maxConnectAttemptTimeMs: 180_000,
		connectPollIntervalMs: 1000,
	});
	const notificationClient = new BaseKafkaClient(
		"notification-client",
		[bootstrap],
		{ metricsEnabled: false, connectWithRetries: true },
	);

	await authClient.connect();
	await notificationClient.connect();

	// Ensure only the specific topic needed for this test
	try {
		const admin = authClient.getAdmin();
		await initializeKafkaTopics(admin, {
			partitions: 3,
			replicationFactor: 1,
			waitForLeadersMs: 15000,
			topics: {
				integrationTopic: {
					name: topic,
					retentionMs: 24 * 60 * 60 * 1000,
					cleanupPolicy: "delete",
				} as any,
			},
		});
		console.log(`Topic ensured: ${topic}`);
	} catch (err: any) {
		console.warn("Topic ensure skipped:", err?.message || err);
	}

	// Notification service subscribes and processes events
	const consumer = await notificationClient.createConsumer({
		groupId: notificationGroup,
	});
	await consumer.connect();
	await consumer.subscribe({ topic, fromBeginning: true });

	const processed: any[] = [];
	const received = new Promise<void>((resolve, reject) => {
		const to = setTimeout(
			() => reject(new Error("Timeout waiting for notification processing")),
			20000,
		);
		consumer
			.run({
				eachMessage: async ({ message }) => {
					try {
						const raw = message.value?.toString() || "{}";
						const json = JSON.parse(raw);
						// Validate using schema
						const parsed = UserRegisteredSchema.parse(json);
						processed.push({ key: message.key?.toString(), parsed });
						clearTimeout(to);
						resolve();
					} catch (e) {
						clearTimeout(to);
						reject(e);
					}
				},
			})
			.catch(reject);
	});

	// Auth service publishes a user.registered event
	const producer = authClient.getProducer();
	const defineEvent = createServicePublisherFactory(producer, authSource);
	const getPartitionKey = (p: z.infer<typeof UserRegisteredSchema>) => p.email;
	const publishUserRegistered = defineEvent(
		topic,
		UserRegisteredSchema,
		getPartitionKey,
	);

	const event = {
		id: nanoid(),
		email: `user-${nanoid().slice(0, 4)}@example.com`,
		name: "Test User",
	} as any; // service publisher factory will enrich timestamp/metadata

	console.log("Publishing user.registered:", { email: event.email });
	await publishUserRegistered(event);

	await received;
	console.log("Integration Success: notification processed", processed[0]);

	await consumer.disconnect();
	await authClient.disconnect();
	await notificationClient.disconnect();
}

main().catch((err) => {
	console.error("Integration E2E Failed:", err);
	process.exit(1);
});
