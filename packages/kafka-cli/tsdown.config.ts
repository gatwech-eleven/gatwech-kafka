import { defineConfig } from "tsdown";

export default defineConfig({
	outDir: "dist",
	format: ["esm"],
	entry: ["./src/index.ts", "./src/types/index.ts"],
	external: [
		"kafkajs",
		"winston",
		"prom-client",
		"buffer",
		"zod",
		"@gatwech/utils",
		"@gatwech/kafka",
	],
	clean: true,
	dts: true,
});
