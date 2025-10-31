import { defineConfig } from "tsup";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/client/index.ts",
    "src/types/index.ts",
    "src/config/topics/index.ts",
    "src/config/ensure-topics.ts",
  ],
  dts: true,
  format: ["cjs", "esm"],
  target: "node20",
  outDir: "dist",
  sourcemap: false,
  clean: true,
  splitting: false,
  legacyOutput: false,
  external: ["kafkajs", "winston", "prom-client"],
});
