import { z } from '@payvo/utils/zod';

declare const TopicConfigSchema: any;
type TopicConfig = z.infer<typeof TopicConfigSchema>;

export { type TopicConfig as T, TopicConfigSchema as a };
