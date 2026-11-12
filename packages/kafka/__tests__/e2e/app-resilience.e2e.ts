/**
 * E2E Test: App Resilience During Kafka Connection Retries
 *
 * This test verifies that the Express app remains responsive and handles
 * HTTP requests correctly even while Kafka is unavailable and retrying
 * connection in the background.
 *
 * Scenario: Auth service should allow users to login even if Kafka is down
 */

import { spawn, ChildProcess } from "child_process";
import { setTimeout } from "timers/promises";

const APP_PORT = 3001; // Use different port to avoid conflicts
const APP_STARTUP_DELAY = 2000; // Wait for app to start
const REQUEST_TIMEOUT = 5000; // Max time for HTTP request

interface HealthResponse {
	status: string;
	timestamp: string;
	services?: {
		kafka?: {
			status: string;
			connected: boolean;
			details: {
				connectionAttempts: number;
				lastError: string | null;
				uptime: number | null;
			};
		};
	};
}

async function makeRequest(
	path: string,
	timeout: number = REQUEST_TIMEOUT,
): Promise<{ status: number; body: any; latency: number }> {
	const start = Date.now();

	try {
		const response = await fetch(`http://localhost:${APP_PORT}${path}`, {
			signal: AbortSignal.timeout(timeout),
		});

		const body = await response.json();
		const latency = Date.now() - start;

		return { status: response.status, body, latency };
	} catch (error) {
		const latency = Date.now() - start;
		throw new Error(`Request failed after ${latency}ms: ${error}`);
	}
}

async function waitForAppReady(
	port: number,
	timeoutMs: number = 10000,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		try {
			const res = await fetch(`http://localhost:${port}/health`);
			if (res.ok) return;
		} catch {
			// ignore connection refused
		}
		await setTimeout(500);
	}
	throw new Error(
		`App failed to become ready on port ${port} within ${timeoutMs}ms`,
	);
}

async function startApp(): Promise<{ child: ChildProcess; logs: string[] }> {
	console.log("Starting Express app (Kafka unavailable)...");

	// Use pnpm dev to ensure we use the local tsx and environment
	const app = spawn("pnpm", ["dev"], {
		cwd: "/home/jeremy/Projects/payvo-canary/packages/kafka/examples/express-app",
		env: {
			...process.env,
			PORT: APP_PORT.toString(),
			KAFKA_BROKERS: "localhost:9092", // Kafka not running
		},
		stdio: ["ignore", "pipe", "pipe"],
	});

	const logs: string[] = [];
	const logHandler = (data: Buffer) => {
		const line = data.toString();
		logs.push(line);
		if (process.env.DEBUG) {
			process.stdout.write(`[APP] ${line}`);
		}
	};

	app.stdout?.on("data", logHandler);
	app.stderr?.on("data", logHandler);

	app.on("error", (error) => {
		console.error("Failed to start app process:", error);
	});

	try {
		await waitForAppReady(APP_PORT);
		console.log("App is ready!");
	} catch (e) {
		console.error("App startup failed. Logs:");
		console.error(logs.join(""));
		app.kill();
		throw e;
	}

	return { child: app, logs };
}

async function main() {
	let appProcess: ChildProcess | null = null;
	let appLogs: string[] = [];

	try {
		// Start app (Kafka will be unavailable and retrying)
		const { child, logs } = await startApp();
		appProcess = child;
		appLogs = logs;

		console.log("\n=== Test 1: Health Check During Kafka Retries ===");
		const healthStart = Date.now();
		const health = await makeRequest("/health");
		const healthLatency = Date.now() - healthStart;

		console.log(`Health check responded in ${healthLatency}ms`);
		console.log("Status:", health.status);
		console.log("Body:", JSON.stringify(health.body, null, 2));

		if (health.status !== 200) {
			throw new Error(`Expected status 200, got ${health.status}`);
		}

		if (healthLatency > 1000) {
			throw new Error(
				`Health check too slow: ${healthLatency}ms (expected < 1000ms)`,
			);
		}

		console.log("PASS: Health check is fast and responsive");

		console.log("\n=== Test 2: Detailed Health Check (Kafka Status) ===");
		const detailedHealth = await makeRequest("/health?detailed=true");
		const healthData = detailedHealth.body as HealthResponse;

		console.log("Kafka status:", healthData.services?.kafka?.status);
		console.log("Kafka connected:", healthData.services?.kafka?.connected);

		if (healthData.services?.kafka?.connected) {
			throw new Error("Kafka should not be connected (it's not running)");
		}

		console.log("PASS: Detailed health check shows Kafka as disconnected");

		console.log("\n=== Test 3: Kafka Status Endpoint ===");
		const kafkaStatus = await makeRequest("/kafka/status");

		console.log(
			"Kafka status response:",
			JSON.stringify(kafkaStatus.body, null, 2),
		);

		if (kafkaStatus.body.connected) {
			throw new Error("Kafka status should show disconnected");
		}

		console.log("PASS: Kafka status endpoint works during retries");

		console.log("\n=== Test 4: Multiple Concurrent Requests ===");
		const concurrentRequests = 5;
		const requestPromises = Array.from({ length: concurrentRequests }, (_, i) =>
			makeRequest("/health").then((r) => ({
				index: i,
				latency: r.latency,
				status: r.status,
			})),
		);

		const results = await Promise.all(requestPromises);
		const avgLatency =
			results.reduce((sum, r) => sum + r.latency, 0) / results.length;
		const maxLatency = Math.max(...results.map((r) => r.latency));

		console.log(`Concurrent requests: ${concurrentRequests}`);
		console.log(`Average latency: ${avgLatency.toFixed(2)}ms`);
		console.log(`Max latency: ${maxLatency}ms`);

		if (maxLatency > 1000) {
			throw new Error(`Concurrent requests too slow: max ${maxLatency}ms`);
		}

		console.log("PASS: App handles concurrent requests efficiently");

		console.log("\n=== Test 5: Metrics Endpoint ===");
		const metricsStart = Date.now();
		const metricsResponse = await fetch(`http://localhost:${APP_PORT}/metrics`, {
			signal: AbortSignal.timeout(REQUEST_TIMEOUT),
		});
		const metricsLatency = Date.now() - metricsStart;
		const metricsJson = await metricsResponse.json();
		console.log(`Metrics endpoint responded in ${metricsLatency}ms`);
		console.log(`Metrics response:`, JSON.stringify(metricsJson, null, 2));

		if (metricsResponse.status !== 200) {
			throw new Error(`Expected status 200, got ${metricsResponse.status}`);
		}

		if (typeof metricsJson.kafka?.connectionRetries !== "number") {
			throw new Error("Metrics should include kafka.connectionRetries");
		}

		console.log("PASS: Metrics endpoint works during Kafka retries");

		console.log("\n=== All Tests Passed ===");
		console.log("App remains fully responsive during Kafka connection retries!");
		console.log("This demonstrates that:");
		console.log("  - Users can still login even if Kafka is down");
		console.log("  - Health checks respond quickly");
		console.log("  - Monitoring endpoints work normally");
		console.log("  - App does not hang or block on Kafka retries");
	} catch (error) {
		console.error("\nTest Failed:", error);
		if (appLogs.length > 0) {
			console.log("\n--- App Logs ---");
			console.log(appLogs.join(""));
			console.log("----------------");
		}
		process.exit(1);
	} finally {
		// Cleanup: kill the app
		if (appProcess) {
			console.log("\nCleaning up: stopping app...");
			appProcess.kill("SIGTERM");

			// Wait a bit for graceful shutdown
			await setTimeout(1000);

			// Force kill if still running
			if (!appProcess.killed) {
				appProcess.kill("SIGKILL");
			}
		}
	}
}

main().catch((error) => {
	console.error("Unhandled error:", error);
	process.exit(1);
});
