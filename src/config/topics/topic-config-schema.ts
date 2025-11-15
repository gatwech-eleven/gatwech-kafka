import { z } from "@gatwech/utils/zod";

export const TopicConfigSchema = z.object({
  name: z.string(),
  retentionMs: z.number().default(604_800_000),
  cleanupPolicy: z
    .enum(["delete", "compact", "compact,delete"])
    .default("delete"),
});

export type TopicConfig = z.infer<typeof TopicConfigSchema>;
