import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import fs from "fs/promises";
import path from "path";
import { initCommand } from "../../src/commands/init";

describe("init command", () => {
	const testDir = path.join(process.cwd(), "__test-output__");

	beforeEach(async () => {
		// Create test directory
		await fs.mkdir(testDir, { recursive: true });
		process.chdir(testDir);
	});

	afterEach(async () => {
		// Cleanup
		process.chdir(path.join(testDir, ".."));
		await fs.rm(testDir, { recursive: true, force: true });
	});

	it("should create src directory", async () => {
		await initCommand({
			name: "test-app",
			brokers: "localhost:9092",
			docker: false,
		});

		const srcExists = await fs
			.access("src")
			.then(() => true)
			.catch(() => false);

		expect(srcExists).toBe(true);
	});

	it("should create kafka.config.ts with correct content", async () => {
		await initCommand({
			name: "my-service",
			brokers: "broker1:9092,broker2:9092",
			docker: false,
		});

		const content = await fs.readFile("src/kafka.config.ts", "utf-8");

		expect(content).toContain("my-service");
		expect(content).toContain("broker1:9092,broker2:9092");
		expect(content).toContain("createKafkaClient");
		expect(content).toContain("resilience");
	});

	it("should create topics.ts file", async () => {
		await initCommand({
			name: "test-app",
			brokers: "localhost:9092",
			docker: false,
		});

		const topicsExists = await fs
			.access("src/topics.ts")
			.then(() => true)
			.catch(() => false);

		expect(topicsExists).toBe(true);
	});

	it("should create .env file with KAFKA_BROKERS", async () => {
		await initCommand({
			name: "test-app",
			brokers: "localhost:9092",
			docker: false,
		});

		const envContent = await fs.readFile(".env", "utf-8");

		expect(envContent).toContain("KAFKA_BROKERS=localhost:9092");
	});

	it("should create docker-compose.yaml when docker option is true", async () => {
		await initCommand({
			name: "test-app",
			brokers: "localhost:9092",
			docker: true,
		});

		const dockerExists = await fs
			.access("docker-compose.yaml")
			.then(() => true)
			.catch(() => false);

		expect(dockerExists).toBe(true);
	});

	it("should not create docker-compose.yaml when docker option is false", async () => {
		await initCommand({
			name: "test-app",
			brokers: "localhost:9092",
			docker: false,
		});

		const dockerExists = await fs
			.access("docker-compose.yaml")
			.then(() => true)
			.catch(() => false);

		expect(dockerExists).toBe(false);
	});

	it("should use default values when options are not provided", async () => {
		await initCommand({});

		const configContent = await fs.readFile("src/kafka.config.ts", "utf-8");

		expect(configContent).toContain("my-kafka-app"); // default name
		expect(configContent).toContain("localhost:9092"); // default broker
	});
});
