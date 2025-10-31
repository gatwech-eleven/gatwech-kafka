import { z } from 'zod';
import { TKafkaMessage } from '../types.js';
import { K as KafkaClient } from './kafka.BmhNfoBx.js';

/** Validate and send a single message using KafkaClient. Throws on invalid payload. */
declare function validateAndSend<T>(client: KafkaClient, topic: string, schema: z.ZodType<T>, message: TKafkaMessage<T>): Promise<void>;
/** Validate and send multiple messages. Any invalid message will abort sending. */
declare function validateAndSendMany<T>(client: KafkaClient, topic: string, schema: z.ZodType<T>, messages: TKafkaMessage<T>[]): Promise<void>;
declare const _default: {
    validateAndSend: typeof validateAndSend;
    validateAndSendMany: typeof validateAndSendMany;
};

declare const validateAndSend$1_validateAndSend: typeof validateAndSend;
declare const validateAndSend$1_validateAndSendMany: typeof validateAndSendMany;
declare namespace validateAndSend$1 {
  export {
    _default as default,
    validateAndSend$1_validateAndSend as validateAndSend,
    validateAndSend$1_validateAndSendMany as validateAndSendMany,
  };
}

export { _default as _, validateAndSend as a, validateAndSendMany as b, validateAndSend$1 as v };
