#!/usr/bin/env node
import { Command } from "commander";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import chalk from "chalk";
import Table from "cli-table3";
import "dotenv/config";

//#region src/commands/init.ts
const __filename$2 = fileURLToPath(import.meta.url);
const __dirname$2 = path.dirname(__filename$2);
async function initCommand(options = {}) {
	const config = {
		name: options.name || "my-kafka-app",
		brokers: options.brokers || "localhost:9092",
		docker: options.docker ?? false
	};
	await fs.mkdir("src", { recursive: true });
	await fs.mkdir("src/producers", { recursive: true });
	await fs.mkdir("src/consumers", { recursive: true });
	await generateKafkaConfig(config);
	await generateTopicsFile(config);
	await generateEnvFile(config);
	if (config.docker) await generateDockerCompose(config);
}
async function generateKafkaConfig(config) {
	const templatePath = path.join(__dirname$2, "../../templates/kafka.config.ts.template");
	const content = (await fs.readFile(templatePath, "utf-8")).replace(/\{\{PROJECT_NAME\}\}/g, config.name).replace(/\{\{BROKERS\}\}/g, config.brokers);
	await fs.writeFile("src/kafka.config.ts", content);
}
async function generateTopicsFile(config) {
	const templatePath = path.join(__dirname$2, "../../templates/topics.ts.template");
	const template = await fs.readFile(templatePath, "utf-8");
	await fs.writeFile("src/topics.ts", template);
}
async function generateEnvFile(config) {
	const templatePath = path.join(__dirname$2, "../../templates/.env.template");
	const content = (await fs.readFile(templatePath, "utf-8")).replace(/\{\{PROJECT_NAME\}\}/g, config.name).replace(/\{\{BROKERS\}\}/g, config.brokers);
	await fs.writeFile(".env", content);
}
async function generateDockerCompose(config) {
	const templatePath = path.join(__dirname$2, "../../templates/docker-compose.yaml.template");
	const template = await fs.readFile(templatePath, "utf-8");
	await fs.writeFile("docker-compose.yaml", template);
}

//#endregion
//#region src/commands/topics/list.ts
async function listTopics() {
	const topicsPath = path.resolve(process.cwd(), "src/topics.ts");
	try {
		const topics$1 = (await import(pathToFileURL(topicsPath).href)).topics;
		if (!topics$1) {
			console.error(chalk.red("Error: No topics definition found in src/topics.ts"));
			return;
		}
		const table = new Table({
			head: [
				chalk.cyan("Group"),
				chalk.cyan("Key"),
				chalk.cyan("Name"),
				chalk.cyan("Partitions"),
				chalk.cyan("Replication")
			],
			style: {
				head: [],
				border: []
			}
		});
		for (const [group, topicGroup] of Object.entries(topics$1)) for (const [key, config] of Object.entries(topicGroup)) table.push([
			group,
			key,
			config.name,
			config.partitions || "-",
			config.replicationFactor || "-"
		]);
		console.log(chalk.bold("\nConfigured Kafka Topics:"));
		console.log(table.toString());
	} catch (error) {
		if (error.code === "ERR_MODULE_NOT_FOUND") console.error(chalk.red("Error: src/topics.ts not found. Run 'kafka-cli init' first."));
		else console.error(chalk.red(`Error: ${error.message}`));
	}
}

//#endregion
//#region src/commands/topics/create.ts
async function createTopics() {
	const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");
	const topicsPath = path.resolve(process.cwd(), "src/topics.ts");
	try {
		const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);
		const { topics: topics$1 } = await import(pathToFileURL(topicsPath).href);
		if (!kafka || !topics$1) {
			console.error(chalk.red("Error: Could not find kafka instance or topics definition."));
			return;
		}
		console.log(chalk.blue("Connecting to Kafka..."));
		await kafka.connect();
		console.log(chalk.blue("Provisioning topics..."));
		await kafka.provisionTopics();
		console.log(chalk.green("✔ Topics provisioned successfully."));
		await kafka.disconnect();
	} catch (error) {
		console.error(chalk.red(`Error: ${error.message}`));
		if (error.code === "ERR_MODULE_NOT_FOUND") console.log(chalk.yellow("Hint: Ensure 'src/kafka.config.ts' and 'src/topics.ts' exist. Run 'kafka-cli init' to create them."));
	}
}

//#endregion
//#region src/commands/topics/describe.ts
async function describeTopic(topicName) {
	const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");
	try {
		const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);
		if (!kafka) {
			console.error(chalk.red("Error: Could not find kafka instance in src/kafka.config.ts"));
			return;
		}
		console.log(chalk.blue(`Fetching metadata for topic "${topicName}"...`));
		await kafka.connect();
		const topicMetadata = (await kafka.getAdmin().fetchTopicMetadata({ topics: [topicName] })).topics[0];
		if (!topicMetadata) {
			console.error(chalk.red(`Error: Topic "${topicName}" not found in cluster.`));
			await kafka.disconnect();
			return;
		}
		console.log(chalk.bold(`\nTopic: ${chalk.green(topicMetadata.name)}`));
		const table = new Table({
			head: [
				chalk.cyan("Partition"),
				chalk.cyan("Leader"),
				chalk.cyan("Replicas"),
				chalk.cyan("ISRs")
			],
			style: {
				head: [],
				border: []
			}
		});
		topicMetadata.partitions.forEach((p) => {
			table.push([
				p.partitionId,
				p.leader,
				p.replicas.join(", "),
				p.isr.join(", ")
			]);
		});
		console.log(table.toString());
		await kafka.disconnect();
	} catch (error) {
		console.error(chalk.red(`Error: ${error.message}`));
	}
}

//#endregion
//#region src/commands/consumer/start.ts
async function startConsumer(topic, options) {
	const kafkaConfigPath = path.resolve(process.cwd(), "src/kafka.config.ts");
	const groupId = options.group || `kafka-cli-group-${Math.floor(Math.random() * 1e4)}`;
	try {
		const { kafka } = await import(pathToFileURL(kafkaConfigPath).href);
		if (!kafka) {
			console.error(chalk.red("Error: Could not find kafka instance in src/kafka.config.ts"));
			return;
		}
		console.log(chalk.blue(`Starting consumer for topic "${chalk.bold(topic)}" with group ID "${chalk.bold(groupId)}"...`));
		await kafka.connect();
		const consumer = kafka.getConsumer(groupId);
		await consumer.connect();
		await consumer.subscribe({
			topic,
			fromBeginning: false
		});
		console.log(chalk.green("✔ Consumer started. Waiting for messages... (Press Ctrl+C to stop)"));
		await consumer.run({ eachMessage: async ({ topic: topic$1, partition, message }) => {
			const prefix = `${chalk.dim((/* @__PURE__ */ new Date()).toISOString())} [${chalk.cyan(topic$1)}:${chalk.yellow(partition)}]`;
			const key = message.key ? chalk.magenta(message.key.toString()) : chalk.dim("no-key");
			const value = message.value ? message.value.toString() : chalk.dim("no-value");
			console.log(`${prefix} key: ${key} | value: ${value}`);
			if (message.headers) {
				const headersStr = Object.entries(message.headers).map(([k, v]) => `${k}=${v?.toString()}`).join(", ");
				if (headersStr) console.log(`  ${chalk.dim("Headers:")} ${headersStr}`);
			}
		} });
		const shutdown = async () => {
			console.log(chalk.yellow("\nDisconnecting consumer..."));
			await consumer.disconnect();
			await kafka.disconnect();
			process.exit(0);
		};
		process.on("SIGINT", shutdown);
		process.on("SIGTERM", shutdown);
	} catch (error) {
		console.error(chalk.red(`Error: ${error.message}`));
	}
}

//#endregion
//#region src/commands/generate/consumer.ts
const __filename$1 = fileURLToPath(import.meta.url);
const __dirname$1 = path.dirname(__filename$1);
async function generateConsumer(name, options) {
	const topic = options.topic || `${name.toLowerCase()}-events`;
	const groupId = options.group || `${name.toLowerCase()}-group`;
	const templatePath = path.join(__dirname$1, "../../../templates/consumer.ts.template");
	const content = (await fs.readFile(templatePath, "utf-8")).replace(/\{\{NAME\}\}/g, name).replace(/\{\{TOPIC\}\}/g, topic).replace(/\{\{GROUP_ID\}\}/g, groupId);
	const targetDir = path.resolve(process.cwd(), "src/consumers");
	await fs.mkdir(targetDir, { recursive: true });
	const targetFile = path.join(targetDir, `${name.toLowerCase()}.ts`);
	await fs.writeFile(targetFile, content);
	console.log(chalk.green(`✔ Consumer generated: ${chalk.bold(targetFile)}`));
}

//#endregion
//#region src/commands/generate/producer.ts
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
async function generateProducer(name, options) {
	const topic = options.topic || `${name.toLowerCase()}-events`;
	const templatePath = path.join(__dirname, "../../../templates/producer.ts.template");
	const content = (await fs.readFile(templatePath, "utf-8")).replace(/\{\{NAME\}\}/g, name).replace(/\{\{TOPIC\}\}/g, topic);
	const targetDir = path.resolve(process.cwd(), "src/producers");
	await fs.mkdir(targetDir, { recursive: true });
	const targetFile = path.join(targetDir, `${name.toLowerCase()}.ts`);
	await fs.writeFile(targetFile, content);
	console.log(chalk.green(`✔ Producer generated: ${chalk.bold(targetFile)}`));
}

//#endregion
//#region src/index.ts
const program = new Command();
program.name("kafka-cli").description("CLI tool for @gatwech/kafka").version("0.1.0");
program.command("init").description("Initialize a new Kafka project").option("--name <name>", "Project name").option("--brokers <brokers>", "Kafka brokers").option("--docker", "Include docker-compose.yaml").action(initCommand);
const topics = program.command("topics").description("Manage Kafka topics");
topics.command("list").description("List configured topics").action(listTopics);
topics.command("create").description("Create missing topics").action(createTopics);
topics.command("describe <topic>").description("Describe a topic metadata").action(describeTopic);
program.command("consumer").description("Manage Kafka consumers").command("start <topic>").description("Start a simple consumer for a topic").option("--group <group>", "Consumer group ID").action(startConsumer);
const generate = program.command("generate").description("Generate boilerplate code");
generate.command("consumer <name>").description("Generate a new consumer").option("--topic <topic>", "Kafka topic").option("--group <group>", "Consumer group ID").action(generateConsumer);
generate.command("producer <name>").description("Generate a new producer").option("--topic <topic>", "Kafka topic").action(generateProducer);
program.parse();

//#endregion
export {  };