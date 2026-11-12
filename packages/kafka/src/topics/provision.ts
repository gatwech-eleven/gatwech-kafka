import type { InternalLogger } from "@gatwech/core/env";
import kafkajs, { type Admin } from "kafkajs";

// Node's ESM doesn't find ConfigResourceTypes by name, so it is read from the
// default export, which is the whole module
const { ConfigResourceTypes } = kafkajs;

const RETRY_DELAY_BASE_MS = 250;
const RETRY_DELAY_CAP_MS = 2_000;

const sleep = (ms: number) =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

const messageOf = (error: unknown) =>
	error instanceof Error ? error.message : String(error);

/**
 * Calls the function again after a failure, up to `attempts` times in all,
 * waiting 250 ms and then twice as long each time, up to 2 seconds
 */
async function withRetry<T>(fn: () => Promise<T>, attempts = 5): Promise<T> {
	for (let failures = 1; ; failures++) {
		try {
			return await fn();
		} catch (error) {
			if (failures >= attempts) {
				throw error;
			}
			await sleep(
				Math.min(RETRY_DELAY_BASE_MS * 2 ** (failures - 1), RETRY_DELAY_CAP_MS),
			);
		}
	}
}

/**
 * Waits until every partition of a topic has a leader. Gives up with a warning
 * once `timeoutMs` has passed
 */
async function waitForLeaders(options: {
	admin: Admin;
	logger: InternalLogger;
	topic: string;
	partitions: number;
	timeoutMs: number;
}) {
	const { admin, logger, topic, partitions } = options;
	const deadline = Date.now() + options.timeoutMs;

	while (true) {
		// A topic another client has just created may not be known to this
		// broker yet, which it answers with an error
		const metadata = await admin
			.fetchTopicMetadata({ topics: [topic] })
			.catch(() => undefined);
		const found = metadata?.topics.find((candidate) => candidate.name === topic);
		const hasLeaders =
			found !== undefined &&
			found.partitions.length >= partitions &&
			found.partitions.every((partition) => partition.leader !== -1);

		if (hasLeaders) {
			return;
		}
		if (Date.now() >= deadline) {
			logger.warn("Gave up waiting for the leaders of a topic", {
				topic,
				have: found?.partitions.length,
				expected: partitions,
			});
			return;
		}
		await sleep(250);
	}
}

/**
 * Brings the topics on the broker in line with the ones given
 * @description Creates the ones that are missing, raises the partitions of the
 * ones that have fewer than wanted (Kafka can't lower them), and sets their
 * retention and cleanup policy when those differ. A client that can't change
 * the configs of a topic (the ACLs, or an old broker) logs a warning and goes on
 */
export async function provisionTopics(options: {
	admin: Admin;
	logger: InternalLogger;
	/**
	 * The topics by their names on the broker, each once
	 */
	topics: {
		name: string;
		partitions?: number | undefined;
		replicationFactor?: number | undefined;
		retentionMs: number;
		cleanupPolicy: string;
	}[];
	/**
	 * How many partitions a topic that doesn't say has
	 */
	partitions: number;
	replicationFactor: number;
	waitForLeadersMs: number;
}) {
	const { admin, logger } = options;

	const wanted = options.topics.map((topic) => ({
		name: topic.name,
		partitions: topic.partitions ?? options.partitions,
		replicationFactor: topic.replicationFactor ?? options.replicationFactor,
		configEntries: [
			{ name: "retention.ms", value: String(topic.retentionMs) },
			{ name: "cleanup.policy", value: topic.cleanupPolicy },
		],
	}));

	const existing = await withRetry(() => admin.fetchTopicMetadata());
	const missing = wanted.filter(
		(topic) => !existing.topics.some((found) => found.name === topic.name),
	);
	if (missing.length > 0) {
		logger.info("Creating Kafka topics", {
			topics: missing.map((topic) => topic.name),
		});
		// kafkajs answers false, and doesn't throw, for topics that exist, so two
		// services starting together don't clash
		await withRetry(() =>
			admin.createTopics({
				topics: missing.map((topic) => ({
					topic: topic.name,
					numPartitions: topic.partitions,
					replicationFactor: topic.replicationFactor,
					configEntries: topic.configEntries,
				})),
				waitForLeaders: true,
			}),
		);
		for (const topic of missing) {
			await waitForLeaders({
				admin,
				logger,
				topic: topic.name,
				partitions: topic.partitions,
				timeoutMs: options.waitForLeadersMs,
			});
		}
	}

	const current = await withRetry(() => admin.fetchTopicMetadata());
	const growing = wanted
		.map((topic) => ({
			topic: topic.name,
			count: topic.partitions,
			have:
				current.topics.find((found) => found.name === topic.name)?.partitions
					.length ?? 0,
		}))
		.filter((topic) => topic.count > topic.have);
	if (growing.length > 0) {
		logger.info("Raising the partitions of Kafka topics", { topics: growing });
		try {
			await withRetry(() =>
				admin.createPartitions({
					topicPartitions: growing.map(({ topic, count }) => ({
						topic,
						count,
					})),
				}),
			);
		} catch (error) {
			// Another service may have raised them first
			logger.warn("Could not raise the partitions; going on", {
				error: messageOf(error),
			});
		}
		for (const topic of growing) {
			await waitForLeaders({
				admin,
				logger,
				topic: topic.topic,
				partitions: topic.count,
				timeoutMs: options.waitForLeadersMs,
			});
		}
	}

	let described: Awaited<ReturnType<Admin["describeConfigs"]>> | undefined;
	try {
		described = await admin.describeConfigs({
			resources: wanted.map((topic) => ({
				type: ConfigResourceTypes.TOPIC,
				name: topic.name,
			})),
			includeSynonyms: false,
		});
	} catch {
		// Without the right ACLs everything is set, which may fail the same way
	}

	const differing = wanted
		.map((topic) => {
			const configs = described?.resources.find(
				(resource) => resource.resourceName === topic.name,
			)?.configEntries;
			return {
				type: ConfigResourceTypes.TOPIC,
				name: topic.name,
				configEntries: topic.configEntries.filter(
					(entry) =>
						configs?.find((config) => config.configName === entry.name)
							?.configValue !== entry.value,
				),
			};
		})
		.filter((topic) => topic.configEntries.length > 0);
	if (differing.length === 0) {
		return;
	}
	try {
		await admin.alterConfigs({ resources: differing, validateOnly: false });
		logger.info("Set the configs of Kafka topics", {
			topics: differing.map((topic) => topic.name),
		});
	} catch (error) {
		logger.warn(
			"Could not set the configs of the topics (check the ACLs and the broker version); the topics exist",
			{ error: messageOf(error) },
		);
	}
}
