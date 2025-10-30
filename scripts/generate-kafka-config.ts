import fs from "fs";
import path from "path";
import { topics } from "../src/config/topics";

type TopicConfigRaw = {
  name: string;
  retentionMs?: number;
  cleanupPolicy?: string;
};

const out: Record<string, TopicConfigRaw> = {};

for (const groupKey of Object.keys(topics)) {
  const group = (topics as any)[groupKey] as Record<string, TopicConfigRaw>;
  for (const k of Object.keys(group)) {
    const cfg = group[k];
    if (cfg && cfg.name) {
      out[cfg.name] = {
        name: cfg.name,
        retentionMs: cfg.retentionMs ?? 604_800_000,
        cleanupPolicy: cfg.cleanupPolicy ?? "compact",
      };
    }
  }
}

const __filename = new URL(import.meta.url).pathname;
const __dirname = path.dirname(__filename);
const outPath = path.resolve(__dirname, "..", "kafka-topics-config.json");
fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.info(`Wrote ${Object.keys(out).length} topics to ${outPath}`);
