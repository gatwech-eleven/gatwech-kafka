import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import type { InitOptions } from "../types/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function initCommand(options: InitOptions = {}) {
	// Set defaults
	const config = {
		name: options.name || "my-kafka-app",
		brokers: options.brokers || "localhost:9092",
		docker: options.docker ?? false,
	};

	// Create directories
	await fs.mkdir("src", { recursive: true });
	await fs.mkdir("src/producers", { recursive: true });
	await fs.mkdir("src/consumers", { recursive: true });

	// Generate kafka.config.ts
	await generateKafkaConfig(config);

	// Generate topics.ts
	await generateTopicsFile(config);

	// Generate .env
	await generateEnvFile(config);

	// Generate docker-compose.yaml (if requested)
	if (config.docker) {
		await generateDockerCompose(config);
	}
}

async function generateKafkaConfig(config: { name: string; brokers: string }) {
	const templatePath = path.join(
		__dirname,
		"../../templates/kafka.config.ts.template",
	);
	const template = await fs.readFile(templatePath, "utf-8");

	const content = template
		.replace(/\{\{PROJECT_NAME\}\}/g, config.name)
		.replace(/\{\{BROKERS\}\}/g, config.brokers);

	await fs.writeFile("src/kafka.config.ts", content);
}

async function generateTopicsFile(config: { name: string }) {
	const templatePath = path.join(
		__dirname,
		"../../templates/topics.ts.template",
	);
	const template = await fs.readFile(templatePath, "utf-8");

	await fs.writeFile("src/topics.ts", template);
}

async function generateEnvFile(config: { name: string; brokers: string }) {
	const templatePath = path.join(__dirname, "../../templates/.env.template");
	const template = await fs.readFile(templatePath, "utf-8");

	const content = template
		.replace(/\{\{PROJECT_NAME\}\}/g, config.name)
		.replace(/\{\{BROKERS\}\}/g, config.brokers);

	await fs.writeFile(".env", content);
}

async function generateDockerCompose(config: { name: string }) {
	const templatePath = path.join(
		__dirname,
		"../../templates/docker-compose.yaml.template",
	);
	const template = await fs.readFile(templatePath, "utf-8");

	await fs.writeFile("docker-compose.yaml", template);
}
