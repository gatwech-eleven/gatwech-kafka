import { z } from '@payvo/utils/zod';

declare const TopicConfigSchema: any;
type TopicConfig = z.infer<typeof TopicConfigSchema>;

export { TopicConfigSchema as a };
export type { TopicConfig as T };
