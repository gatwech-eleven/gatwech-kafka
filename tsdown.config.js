import { defineConfig } from "tsdown";
export default defineConfig({
    outDir: "dist",
    dts: { build: true, incremental: true },
    format: ["esm", "cjs"],
    entry: [
        "./src/index.ts",
        "./src/client/index.ts",
        "./src/types/index.ts",
        "./src/config/topics/index.ts",
        "./src/config/ensure-topics.ts",
    ],
    external: [
        "kafkajs",
        "winston",
        "prom-client",
        "buffer",
        "zod",
        "@gatwech/utils",
    ],
    clean: true,
});
