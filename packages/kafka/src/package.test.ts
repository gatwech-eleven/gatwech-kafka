import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

/**
 * These run Node on the built package, which `pnpm test` builds first
 */
describe("package - built entry points", () => {
	it("should load in Node as an ES module and export the API", () => {
		const exported = execFileSync(
			process.execPath,
			[
				"--input-type=module",
				"-e",
				"import * as kafka from './dist/index.mjs'; console.log(Object.keys(kafka).sort().join(','))",
			],
			{ cwd: resolve(import.meta.dirname, ".."), encoding: "utf8" },
		);

		expect(exported.trim().split(",")).toEqual(
			expect.arrayContaining([
				"KAFKA_ERROR_CODES",
				"KafkaError",
				"createKafka",
				"defineTopics",
			]),
		);
	});

	it("should load in Node as CommonJS and export the API", () => {
		const exported = execFileSync(
			process.execPath,
			[
				"-e",
				"console.log(Object.keys(require('./dist/index.cjs')).sort().join(','))",
			],
			{ cwd: resolve(import.meta.dirname, ".."), encoding: "utf8" },
		);

		expect(exported.trim().split(",")).toEqual(
			expect.arrayContaining([
				"KAFKA_ERROR_CODES",
				"KafkaError",
				"createKafka",
				"defineTopics",
			]),
		);
	});
});
