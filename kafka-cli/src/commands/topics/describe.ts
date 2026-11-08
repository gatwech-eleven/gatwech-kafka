import path from "path";
import { pathToFileURL } from "url";
import chalk from "chalk";
import Table from "cli-table3";
import "dotenv/config";

export async function describeTopic(topicName: string) {
	const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");

	try {
		const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);

		if (!kafka) {
			console.error(
				chalk.red("Error: Could not find kafka instance in src/kafka.config.ts"),
			);
			return;
		}

		console.log(chalk.blue(`Fetching metadata for topic "${topicName}"...`));
		await kafka.connect();

		const admin = kafka.getAdmin();
		const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
		const topicMetadata = metadata.topics[0];

		if (!topicMetadata) {
			console.error(
				chalk.red(`Error: Topic "${topicName}" not found in cluster.`),
			);
			await kafka.disconnect();
			return;
		}

		console.log(chalk.bold(`\nTopic: ${chalk.green(topicMetadata.name)}`));

		const table = new Table({
			head: [
				chalk.cyan("Partition"),
				chalk.cyan("Leader"),
				chalk.cyan("Replicas"),
				chalk.cyan("ISRs"),
			],
			style: { head: [], border: [] },
		});

		topicMetadata.partitions.forEach((p: any) => {
			table.push([
				p.partitionId,
				p.leader,
				p.replicas.join(", "),
				p.isr.join(", "),
			]);
		});

		console.log(table.toString());

		await kafka.disconnect();
	} catch (error: any) {
		console.error(chalk.red(`Error: ${error.message}`));
	}
}
