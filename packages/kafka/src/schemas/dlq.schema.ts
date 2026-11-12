import { z } from "@gatwech/utils/zod";
import { BaseProducerSchema } from "./base.schema";

export const DlqValidationIssueSchema = z.object({
	/**
	 * Where in the message the issue is, joined with dots. Empty when the
	 * message as a whole is at fault
	 */
	path: z.string(),
	message: z.string(),
	code: z.string(),
});

export const DlqMessageSchema = BaseProducerSchema.extend({
	originalTopic: z.string(),
	originalPartition: z.number().int().nonnegative(),
	originalOffset: z.string(),
	consumerGroup: z.string(),
	/**
	 * Why the message was given up on: the handler failed on every attempt, or
	 * the message doesn't match the topic's schema
	 */
	reason: z.enum(["handler", "schema"]),
	/**
	 * The message of the handler's last error, or a summary of the validation
	 * issues
	 */
	errorMessage: z.string(),
	/**
	 * The stack of the handler's last error
	 */
	stackTrace: z.string().optional(),
	/**
	 * The message of the `cause` of the handler's last error
	 */
	errorCause: z.string().optional(),
	/**
	 * What the topic's schema rejected. Only with `reason: "schema"`
	 */
	validationIssues: z.array(DlqValidationIssueSchema).optional(),
	originalKey: z.string().optional(),
	originalHeaders: z.record(z.string(), z.string()).optional(),
	// Keep the original payload as-is; allow any to avoid losing information
	originalValue: z.any(),
});

export type TDlqValidationIssue = z.infer<typeof DlqValidationIssueSchema>;

export type TDlqMessage = z.infer<typeof DlqMessageSchema>;

export default {
	DlqMessageSchema,
};
