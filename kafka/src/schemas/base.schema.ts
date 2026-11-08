import { z } from "@gatwech/utils/zod";

// Common event base fields: enforced across all producer payloads.
// timestamp: epoch milliseconds (number)
// metadata: structured object with optional correlationId & source
export const BaseEventMetadataSchema = z
	.object({
		correlationId: z.string().optional(),
		source: z.string().optional(),
	})
	.optional();

export const BaseProducerSchema = z.object({
	timestamp: z.number().int().nonnegative(),
	metadata: BaseEventMetadataSchema,
});
