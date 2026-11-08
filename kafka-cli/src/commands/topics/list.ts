import path from "path";
import { pathToFileURL } from "url";
import chalk from "chalk";
import Table from "cli-table3";

export async function listTopics() {
	const topicsPath = path.resolve(process.cwd(), "src/topics.ts");

	try {
		// Dynamically import the topics file
		// We use pathToFileURL to ensure it works on Windows and with ESM
		const module = await import(pathToFileURL(topicsPath).href);
		const topics = module.topics;

		if (!topics) {
			console.error(
				chalk.red("Error: No topics definition found in src/topics.ts"),
			);
			return;
		}

		const table = new Table({
			head: [
				chalk.cyan("Group"),
				chalk.cyan("Key"),
				chalk.cyan("Name"),
				chalk.cyan("Partitions"),
				chalk.cyan("Replication"),
			],
			style: { head: [], border: [] },
		});

		for (const [group, topicGroup] of Object.entries(topics)) {
			for (const [key, config] of Object.entries(topicGroup as any)) {
				table.push([
					group,
					key,
					(config as any).name,
					(config as any).partitions || "-",
					(config as any).replicationFactor || "-",
				]);
			}
		}

		console.log(chalk.bold("\nConfigured Kafka Topics:"));
		console.log(table.toString());
	} catch (error: any) {
		if (error.code === "ERR_MODULE_NOT_FOUND") {
			console.error(
				chalk.red("Error: src/topics.ts not found. Run 'kafka-cli init' first."),
			);
		} else {
			console.error(chalk.red(`Error: ${error.message}`));
		}
	}
}
