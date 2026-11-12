import { z } from "@gatwech/utils/zod";
import { describe, expectTypeOf, it } from "vitest";
import type { KafkaConsumer } from "../consumer/types";
import { KAFKA_ERROR_CODES } from "../error-codes";
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

	const clientWith = () =>
		createKafka({
			clientId: "payvo-auth-service",
			brokers: ["localhost:9092"],
			topics: defineTopics({ userRegistered }),
		});

	it("types an event from the schema of the topic it names", () => {
		const check = () => {
			const kafka = clientWith();

			expectTypeOf(kafka.publish).returns.resolves.toBeVoid();
			void kafka.publish({
				topic: "userRegistered",
				event: { userId: "u1", email: "a@b.c" },
			});
			void kafka.publish({
				topic: "userRegistered",
				// @ts-expect-error email is a string
				event: { userId: "u1", email: 1 },
			});
			void kafka.publish({
				topic: "userRegistered",
				// @ts-expect-error userId is missing
				event: { email: "a@b.c" },
			});
			// @ts-expect-error the event goes under `event`
			void kafka.publish({ topic: "userRegistered", userId: "u1" });
		};
		void check;
	});

	it("types every event of a batch from the same schema", () => {
		const check = () => {
			const kafka = clientWith();

			void kafka.publishBatch({
				topic: "userRegistered",
				events: [
					{ userId: "u1", email: "a@b.c" },
					{ userId: "u2", email: "d@e.f" },
				],
			});
			void kafka.publishBatch({
				topic: "userRegistered",
				// @ts-expect-error the second event has no email
				events: [{ userId: "u1", email: "a@b.c" }, { userId: "u2" }],
			});
		};
		void check;
	});

	it("only names topics the client was given", () => {
		const check = () => {
			const kafka = clientWith();

			// @ts-expect-error there is no such topic
			void kafka.publish({ topic: "userDeleted", event: {} });
			void kafka.consume({
				// @ts-expect-error there is no such topic
				topic: "userDeleted",
				handler: async () => {},
			});

			const bare = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
			});
			// @ts-expect-error a client with no topics has none to name
			void bare.publish({ topic: "userRegistered", event: {} });
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
				topic: "userRegistered",
				handler: async ({ event, partition, topic, correlationId, source }) => {
					// The handler gets what the schema outputs, so `age` is parsed
					expectTypeOf(event).toEqualTypeOf<{ userId: string; age: number }>();
					expectTypeOf(partition).toBeNumber();
					expectTypeOf(topic).toBeString();
					expectTypeOf(correlationId).toEqualTypeOf<string | undefined>();
					expectTypeOf(source).toEqualTypeOf<string | undefined>();
					// @ts-expect-error the schema has no such field
					void event.nope;
				},
			});
			expectTypeOf(consumer).resolves.toEqualTypeOf<KafkaConsumer>();
		};
		void check;
	});

	it("reads a topic with nothing but the topic and a handler", () => {
		const check = () => {
			const kafka = clientWith();

			void kafka.consume({
				topic: "userRegistered",
				handler: async ({ event }) => {
					expectTypeOf(event.userId).toBeString();
				},
			});
			void kafka.consume({
				topic: "userRegistered",
				groupId: "payvo-auth-service.legacy-group",
				retry: { attempts: 5 },
				dlq: { publishAttempts: 2 },
				session: { timeout: 45_000 },
				handler: async () => {},
			});
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

	it("lets a topic keep its declared name inside a namespace", () => {
		const check = () => {
			// The auth service's topics, as they are on the broker: two carry the
			// namespace and the rest are bare
			const kafka = createKafka({
				clientId: "payvo-auth-service",
				brokers: ["localhost:9092"],
				namespace: "prod",
				topics: defineTopics({
					userRegistered: { ...userRegistered, namespaced: false },
					kycStatusUpdated: {
						name: "user.kyc.status.updated",
						schema: z.object({ userId: z.string(), status: z.string() }),
						key: (event) => event.userId,
					},
				}),
			});

			expectTypeOf(kafka.topics.userRegistered.namespaced).toBeBoolean();
			void kafka.publish({
				topic: "kycStatusUpdated",
				event: { userId: "u1", status: "approved" },
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
			createKafka({
				clientId: "payvo-auth-service",
				brokers: ["a:9092"],
				enabled: false,
			});
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
