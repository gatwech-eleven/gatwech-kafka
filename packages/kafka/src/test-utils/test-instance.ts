import { randomUUID } from "node:crypto";
import { ConfigResourceTypes, Kafka, logLevel } from "kafkajs";
import { createKafka } from "../client";
import { BaseKafkaClient } from "../client/core/kafka";
import type { KafkaOptions } from "../client/types";

/**
 * A client on a real broker (`KAFKA_BROKERS`, default `localhost:9092`), with
 * topics and groups unique to the call
 *
 * `kafka` is a `createKafka` client of the same broker, with a namespace of its
 * own, that the options override. It is not connected
 *
 * The broker must not create topics on its own, so a missing topic fails
 * as it would in production
 */
export const getTestInstance = async <
	Options extends Partial<KafkaOptions> = Partial<KafkaOptions>,
>(
	options: Options = {} as Options,
) => {
	const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
	const id = randomUUID().slice(0, 8);

	const kafkajs = new Kafka({
		clientId: `test-${id}`,
		brokers,
		logLevel: logLevel.NOTHING,
	});
	const admin = kafkajs.admin();
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
	// Every topic a test provisions starts with this, so they can all be removed
	const namespace = `test${id}`;

	const logs: { level: string; message: string }[] = [];
	const kafka = createKafka({
		clientId: `payvo-test-${id}`,
		brokers,
		namespace,
		connect: { attempts: 2, retryDelayMs: 50 },
		logger: {
			level: "debug",
			log: (level, message) => {
				logs.push({ level, message });
			},
		},
		...options,
	});

	return {
		client,
		kafka,
		brokers,
		namespace,
		/** What the client has logged, in order */
		logs,
		/** How many times the client has logged exactly this message */
		countLogs: (message: string) =>
			logs.filter((entry) => entry.message === message).length,
		/** A name that no other test uses, for a topic that is not namespaced */
		uniqueName: (name: string) => `${name}.${id}`,
		/** What the broker has for a topic, or `undefined` when it doesn't exist */
		describeTopic: async (name: string) => {
			// A broker that doesn't create topics on its own fails a lookup of one
			// that doesn't exist
			if (!(await admin.listTopics()).includes(name)) {
				return undefined;
			}
			const { topics } = await admin.fetchTopicMetadata({ topics: [name] });
			const metadata = topics.find((topic) => topic.name === name);
			if (!metadata || metadata.partitions.length === 0) {
				return undefined;
			}
			const { resources } = await admin.describeConfigs({
				resources: [{ type: ConfigResourceTypes.TOPIC, name }],
				includeSynonyms: false,
			});
			const entry = (key: string) =>
				resources[0]?.configEntries.find((config) => config.configName === key)
					?.configValue;
			return {
				partitions: metadata.partitions.length,
				retentionMs: Number(entry("retention.ms")),
				cleanupPolicy: entry("cleanup.policy"),
			};
		},
		/** The names of the topics on the broker that start with the prefix */
		listTopics: async (prefix: string) =>
			(await admin.listTopics()).filter((name) => name.startsWith(prefix)),
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
		/**
		 * How many messages a group has committed on the topic, over all its
		 * partitions, or `-1` when it has committed none
		 */
		committedOffset: async (groupId: string, topic: string) => {
			const [group] = await admin.fetchOffsets({ groupId, topics: [topic] });
			const offsets = (group?.partitions ?? []).map((partition) =>
				Number(partition.offset),
			);
			return offsets.every((offset) => offset < 0)
				? -1
				: offsets.reduce((sum, offset) => sum + Math.max(offset, 0), 0);
		},
		/** How many consumers are in the group right now */
		groupMembers: async (groupId: string) => {
			const { groups } = await admin.describeGroups([groupId]);
			return groups[0]?.members.length ?? 0;
		},
		/** Every value on the topic, read from the start by a group of its own */
		readTopic: async (topic: string, options: { within: number }) => {
			const reader = kafkajs.consumer({ groupId: `reader.${randomUUID()}` });
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
		/**
		 * Every message on the topic with its key, its headers and its value,
		 * read from the start by a group of its own
		 */
		readMessages: async (topic: string, options: { within: number }) => {
			const reader = kafkajs.consumer({ groupId: `reader.${randomUUID()}` });
			readers.push(reader);
			const messages: {
				key: string | undefined;
				headers: Record<string, string>;
				value: string;
			}[] = [];
			await reader.connect();
			await reader.subscribe({ topic, fromBeginning: true });
			await reader.run({
				eachMessage: async ({ message }) => {
					messages.push({
						key: message.key?.toString(),
						headers: Object.fromEntries(
							Object.entries(message.headers ?? {}).map(([name, value]) => [
								name,
								String(value),
							]),
						),
						value: message.value?.toString() ?? "",
					});
				},
			});
			await new Promise((resolve) => setTimeout(resolve, options.within));
			await reader.disconnect();
			return messages;
		},
		/** Removes a topic and waits until the broker no longer lists it */
		deleteTopic: async (name: string) => {
			await admin.deleteTopics({ topics: [name] });
			const deadline = Date.now() + 15_000;
			while (Date.now() < deadline) {
				if (!(await admin.listTopics()).includes(name)) {
					return;
				}
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
			throw new Error(`The topic ${name} was not deleted`);
		},
		close: async () => {
			await client.disconnect();
			for (const reader of readers) {
				await reader.disconnect().catch(() => undefined);
			}
			await kafka.disconnect();
			const provisioned = (await admin.listTopics()).filter(
				(name) => name.startsWith(`${namespace}.`) || name.endsWith(`.${id}`),
			);
			await admin
				.deleteTopics({ topics: [...createdTopics, ...provisioned] })
				.catch(() => undefined);
			await admin.disconnect();
		},
	};
};
