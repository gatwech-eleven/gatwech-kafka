import { randomUUID } from "node:crypto";
import { Kafka, logLevel } from "kafkajs";
import { BaseKafkaClient } from "../client/core/kafka";

/**
 * A client on a real broker (`KAFKA_BROKERS`, default `localhost:9092`), with
 * topics and groups unique to the call
 *
 * The broker must not create topics on its own, so a missing topic fails
 * as it would in production
 */
export const getTestInstance = async () => {
	const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
	const id = randomUUID().slice(0, 8);

	const kafka = new Kafka({
		clientId: `test-${id}`,
		brokers,
		logLevel: logLevel.NOTHING,
	});
	const admin = kafka.admin();
	await admin.connect();

	const client = new BaseKafkaClient(`test-${id}`, brokers, {
		retryDelayMs: 100,
		maxConnectionRetries: 2,
		allowAutoTopicCreation: false,
		metricsEnabled: false,
	});
	await client.connect();

	const createdTopics: string[] = [];
	const readers: { disconnect: () => Promise<void> }[] = [];

	return {
		client,
		groupId: `group.${id}`,
		/** Creates a one-partition topic and returns its name */
		createTopic: async (name?: string | undefined) => {
			const topic = name ?? `topic.${id}.${createdTopics.length}`;
			await admin.createTopics({
				topics: [{ topic, numPartitions: 1 }],
				waitForLeaders: true,
			});
			createdTopics.push(topic);
			return topic;
		},
		/**
		 * Resolves once the group has a member and is stable. A group with no
		 * committed offset starts at the end of the topic, so messages sent
		 * before this resolves are skipped
		 */
		joined: async (groupId: string) => {
			const deadline = Date.now() + 30_000;
			while (Date.now() < deadline) {
				const { groups } = await admin.describeGroups([groupId]);
				if (groups[0]?.state === "Stable" && groups[0].members.length > 0) {
					// Its start position is chosen just after the group turns stable
					await new Promise((resolve) => setTimeout(resolve, 500));
					return;
				}
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
			throw new Error(`The group ${groupId} did not join`);
		},
		/** Publishes each value as a message on the topic */
		send: async (topic: string, values: string[]) => {
			await client.getProducer().send({
				topic,
				messages: values.map((value) => ({ value })),
			});
		},
		/** The offset a group has committed on the topic's only partition, or `-1` */
		committedOffset: async (groupId: string, topic: string) => {
			const [group] = await admin.fetchOffsets({ groupId, topics: [topic] });
			return Number(group?.partitions[0]?.offset ?? -1);
		},
		/** Every value on the topic, read from the start by a group of its own */
		readTopic: async (topic: string, options: { within: number }) => {
			const reader = kafka.consumer({ groupId: `reader.${randomUUID()}` });
			readers.push(reader);
			const values: string[] = [];
			await reader.connect();
			await reader.subscribe({ topic, fromBeginning: true });
			await reader.run({
				eachMessage: async ({ message }) => {
					values.push(message.value?.toString() ?? "");
				},
			});
			await new Promise((resolve) => setTimeout(resolve, options.within));
			await reader.disconnect();
			return values;
		},
		close: async () => {
			await client.disconnect();
			for (const reader of readers) {
				await reader.disconnect().catch(() => undefined);
			}
			await admin.deleteTopics({ topics: createdTopics }).catch(() => undefined);
			await admin.disconnect();
		},
	};
};
