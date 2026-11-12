import path from "path";
import { pathToFileURL } from "url";
import chalk from "chalk";
import "dotenv/config";

export async function startConsumer(
	topic: string,
	options: { group?: string },
) {
	const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");
	const groupId =
		options.group || `kafka-cli-group-${Math.floor(Math.random() * 10000)}`;

	try {
		const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);

		if (!kafka) {
			console.error(
				chalk.red("Error: Could not find kafka instance in src/kafka.config.ts"),
			);
			return;
		}

		console.log(
			chalk.blue(
				`Starting consumer for topic "${chalk.bold(topic)}" with group ID "${chalk.bold(groupId)}"...`,
			),
		);

		await kafka.connect();
		const consumer = kafka.getConsumer(groupId);

		await consumer.connect();
		await consumer.subscribe({ topic, fromBeginning: false });

		console.log(
			chalk.green(
				"✔ Consumer started. Waiting for messages... (Press Ctrl+C to stop)",
			),
		);

		await consumer.run({
			eachMessage: async ({ topic, partition, message }: any) => {
				const prefix = `${chalk.dim(new Date().toISOString())} [${chalk.cyan(topic)}:${chalk.yellow(partition)}]`;
				const key = message.key
					? chalk.magenta(message.key.toString())
					: chalk.dim("no-key");
				const value = message.value
					? message.value.toString()
					: chalk.dim("no-value");

				console.log(`${prefix} key: ${key} | value: ${value}`);

				if (message.headers) {
					const headersStr = Object.entries(message.headers)
						.map(([k, v]) => `${k}=${v?.toString()}`)
						.join(", ");
					if (headersStr) {
						console.log(`  ${chalk.dim("Headers:")} ${headersStr}`);
					}
				}
			},
		});

		// Handle graceful shutdown
		const shutdown = async () => {
			console.log(chalk.yellow("\nDisconnecting consumer..."));
			await consumer.disconnect();
			await kafka.disconnect();
			process.exit(0);
		};

		process.on("SIGINT", shutdown);
		process.on("SIGTERM", shutdown);
	} catch (error: any) {
		console.error(chalk.red(`Error: ${error.message}`));
	}
}
