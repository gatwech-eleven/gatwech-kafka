import { defineConfig } from "tsdown";

export default defineConfig({
	outDir: "dist",
	format: ["esm", "cjs"],
	entry: ["./src/index.ts", "./src/client/index.ts", "./src/topics/index.ts"],
	external: ["kafkajs", "zod", "@gatwech/utils", "@gatwech/kafka"],
	clean: true,
});
