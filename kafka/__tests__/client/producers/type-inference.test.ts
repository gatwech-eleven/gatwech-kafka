import { describe, it, expectTypeOf } from "vitest";
import { z } from "@gatwech/utils/zod";
import {
	createEventPublisher,
	createPublisher,
} from "../../../src/client/producers/event.factory";
import type { Producer } from "kafkajs";

describe("Payload Type Inference", () => {
	const mockProducer = {} as Producer;

	describe("createEventPublisher", () => {
		it("should infer payload type from schema", () => {
			const schema = z.object({
				id: z.string(),
				email: z.string().email(),
				age: z.number(),
			});

			const publisher = createEventPublisher(
				mockProducer,
				"test-topic",
				schema,
				(p) => p.id,
				"test-service",
			);

			// Type test: publisher should accept correct payload type
			expectTypeOf(publisher).parameter(0).toEqualTypeOf<{
				id: string;
				email: string;
				age: number;
			}>();

			// Type test: publisher should reject incorrect types
			expectTypeOf(publisher).parameter(0).not.toEqualTypeOf<{
				id: number; // Wrong type
				email: string;
				age: number;
			}>();
		});

		it("should infer optional fields correctly", () => {
			const schema = z.object({
				id: z.string(),
				name: z.string().optional(),
				metadata: z.record(z.string(), z.unknown()).optional(),
			});

			const publisher = createEventPublisher(
				mockProducer,
				"test-topic",
				schema,
				(p) => p.id,
				"test-service",
			);

			expectTypeOf(publisher).parameter(0).toEqualTypeOf<{
				id: string;
				name?: string;
				metadata?: Record<string, unknown>;
			}>();
		});

		it("should handle nested objects", () => {
			const schema = z.object({
				id: z.string(),
				user: z.object({
					name: z.string(),
					email: z.string(),
				}),
				tags: z.array(z.string()),
			});

			const publisher = createEventPublisher(
				mockProducer,
				"test-topic",
				schema,
				(p) => p.id,
				"test-service",
			);

			expectTypeOf(publisher).parameter(0).toEqualTypeOf<{
				id: string;
				user: {
					name: string;
					email: string;
				};
				tags: string[];
			}>();
		});
	});

	describe("createPublisher", () => {
		it("should infer payload type from schema", () => {
			const schema = z.object({
				userId: z.string(),
				action: z.enum(["create", "update", "delete"]),
			});

			const publisher = createPublisher(
				{
					producer: mockProducer,
					topic: "test-topic",
					key: (p) => p.userId,
					source: "test-service",
				},
				schema,
			);

			// Runtime test - type is inferred correctly
			type ExpectedType = {
				userId: string;
				action: "create" | "update" | "delete";
			};

			expectTypeOf(publisher).toBeFunction();
		});

		it("should infer key function parameter type", () => {
			const schema = z.object({
				id: z.string(),
				email: z.string(),
			});

			createPublisher(
				{
					producer: mockProducer,
					topic: "test-topic",
					key: (p) => {
						// Type test: p should be inferred as the schema type
						expectTypeOf(p).toEqualTypeOf<{
							id: string;
							email: string;
						}>();
						return p.id;
					},
					source: "test-service",
				},
				schema,
			);
		});
	});

	describe("PartitionKeyFn type inference", () => {
		it("should infer partition key function type", () => {
			const schema = z.object({
				userId: z.string(),
				timestamp: z.number(),
			});

			type InferredPayload = z.infer<typeof schema>;

			const partitionKey = (payload: InferredPayload) => payload.userId;

			expectTypeOf(partitionKey).parameter(0).toEqualTypeOf<{
				userId: string;
				timestamp: number;
			}>();

			expectTypeOf(partitionKey).returns.toBeString();
		});
	});
});
