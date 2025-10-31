import { z } from '@payvo/utils';
import { Producer } from 'kafkajs';

type KeyDeriver<T> = (payload: T) => string;
/**
 * Factory for creating validated event publishers.
 */
declare function createEventPublisher<TSchema extends z.ZodObject<any, any>>(producer: Producer, topicName: string, schema: TSchema, keyDeriver: KeyDeriver<z.infer<TSchema>>, source: string): (payload: z.input<TSchema>) => Promise<void>;

export { createEventPublisher };
