import { z } from "@gatwech/utils/zod";

//#region src/config/topics/topic-config-schema.d.ts
declare const TopicConfigSchema: z.ZodObject<{
  name: z.ZodString;
  retentionMs: z.ZodDefault<z.ZodNumber>;
  cleanupPolicy: z.ZodDefault<z.ZodEnum<{
    delete: "delete";
    compact: "compact";
    "compact,delete": "compact,delete";
  }>>;
}, z.core.$strip>;
type TopicConfig = z.infer<typeof TopicConfigSchema>;
//#endregion
export { TopicConfigSchema as n, TopicConfig as t };