import { z } from "@gatwech/utils/zod";
import { describe, expectTypeOf, it } from "vitest";
import type { KafkaConsumer } from "../consumer/types";
import { KAFKA_ERROR_CODES } from "../error-codes";
import type { PublishResult } from "../publisher/types";
import { defineTopics } from "../topics/types";
import { createKafka } from "./types";

/**
 * The contract is types, so these are checked by `tsc` and never run:
 * `createKafka` and `defineTopics` are declarations until their modules land
 */
describe("kafka - public types", () => {
	const userRegistered = {
		name: "user.registered",
		schema: z.object({ userId: z.string(), email: z.string() }),
		key: (event: { userId: string }) => event.userId,
	};

	it("types an event from the schema of the topic it names", () => {
		const check = () => {
			const topics = defineTopics({ userRegistered });
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics,
			});
			const publisher = kafka.publisher({
				topic: "userRegistered",
				source: "payvo-auth-service",
			});

			expectTypeOf(
				publisher.publish,
			).returns.resolves.toEqualTypeOf<PublishResult>();
			void publisher.publish({ event: { userId: "u1", email: "a@b.c" } });
			// @ts-expect-error email is a string
			void publisher.publish({ event: { userId: "u1", email: 1 } });
			// @ts-expect-error userId is missing
			void publisher.publish({ event: { email: "a@b.c" } });
			// @ts-expect-error a publish takes one object
			void publisher.publish({ userId: "u1", email: "a@b.c" });
		};
		void check;
	});

	it("only names topics the client was given", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({ userRegistered }),
			});

			// @ts-expect-error there is no such topic
			kafka.publisher({ topic: "userDeleted", source: "x" });
			void kafka.consume({
				groupId: "g",
				// @ts-expect-error there is no such topic
				topic: "userDeleted",
				handler: async () => {},
			});

			const bare = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
			});
			// @ts-expect-error a client with no topics has none to name
			bare.publisher({ topic: "userRegistered", source: "x" });
		};
		void check;
	});

	it("types the event a handler receives from the same schema", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-wallet-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({
					userRegistered: {
						name: "user.registered",
						schema: z.object({ userId: z.string(), age: z.coerce.number() }),
						key: (event) => event.userId,
					},
				}),
			});

			const consumer = kafka.consume({
				groupId: "payvo-wallet-service.user-registered",
				topic: "userRegistered",
				handler: async ({ event, partition, topic, correlationId }) => {
					// The handler gets what the schema outputs, so `age` is parsed
					expectTypeOf(event).toEqualTypeOf<{ userId: string; age: number }>();
					expectTypeOf(partition).toBeNumber();
					expectTypeOf(topic).toBeString();
					expectTypeOf(correlationId).toEqualTypeOf<string | undefined>();
					// @ts-expect-error the schema has no such field
					void event.nope;
				},
			});
			expectTypeOf(consumer).resolves.toEqualTypeOf<KafkaConsumer>();
		};
		void check;
	});

	it("types a topic's key from its event", () => {
		const check = () => {
			defineTopics({
				userRegistered: {
					name: "user.registered",
					schema: z.object({ userId: z.string() }),
					// @ts-expect-error the event has no `id`
					key: (event) => event.id,
				},
			});
		};
		void check;
	});

	it("keeps the topics' types on the instance", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				topics: defineTopics({ userRegistered }),
				namespace: "prod",
			});

			expectTypeOf(kafka.topics.userRegistered.name).toBeString();
			expectTypeOf(kafka.topics.userRegistered.retentionMs).toBeNumber();
		};
		void check;
	});

	it("needs a client id and brokers, and nothing else", () => {
		const check = () => {
			createKafka({ clientId: "payvo-auth-service", brokers: ["a:9092"] });
			// @ts-expect-error brokers are required
			createKafka({ clientId: "payvo-auth-service" });
			// @ts-expect-error clientId is required
			createKafka({ brokers: ["a:9092"] });
		};
		void check;
	});

	it("exposes its error codes on the instance", () => {
		const check = () => {
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["a:9092"],
			});
			expectTypeOf(kafka.$ERROR_CODES).toEqualTypeOf<typeof KAFKA_ERROR_CODES>();
			expectTypeOf(kafka.$ERROR_CODES.BROKER_UNREACHABLE).toBeString();
		};
		void check;
	});
});
