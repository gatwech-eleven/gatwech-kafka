import { defineConfig } from "tsdown";

export default defineConfig({
  outDir: "dist",
  format: ["esm", "cjs"],
  entry: [
    "./src/index.ts",
    "./src/client/index.ts",
    "./src/types/index.ts",
    "./src/config/topics/index.ts",
    "./src/config/index.ts",
    "./src/extensions/index.ts",
  ],
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
});
