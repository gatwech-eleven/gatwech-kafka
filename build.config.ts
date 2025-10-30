import { defineBuildConfig } from "unbuild";

export default defineBuildConfig({
  outDir: "dist",
  declaration: true,
  clean: true,
  failOnWarn: false,
  externals: ["kafkajs", "buffer", "winston", "zod", "prom-client"],
  entries: [
    "src/index",
    "src/client",
    "src/client/event.factory",
    "src/schemas",
    "src/types",
    "src/validation",
    "src/producer/validate-and-send",
    "src/config/topics",
    "src/config/ensure-topics",
  ],
});
