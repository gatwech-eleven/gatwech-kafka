import { describe, it, expect, vi } from "vitest";
import { BaseKafkaClient } from "../../../src/client/core/kafka";

// Mock KafkaJS
vi.mock("kafkajs", async () => {
	const actual = await vi.importActual("kafkajs");
	return {
		...actual,
		Kafka: vi.fn(function () {
			return {
				producer: vi.fn().mockReturnValue({
					connect: vi.fn(),
					disconnect: vi.fn(),
					on: vi.fn(),
					send: vi.fn().mockResolvedValue(undefined),
				}),
				admin: vi.fn().mockReturnValue({
					connect: vi.fn(),
					disconnect: vi.fn(),
				}),
			};
		}),
	};
});

describe("Client-Level Defaults", () => {
	it("should store and return default metadata", () => {
		const defaultMetadata = { service: "test-service", version: "1.0.0" };
		const client = new BaseKafkaClient("test-client", ["localhost:9092"], {
			defaultMetadata,
		});

		expect(client.getDefaultMetadata()).toEqual(defaultMetadata);
	});

	it("should store and return default headers", () => {
		const defaultHeaders = { "x-service-id": "test-service" };
		const client = new BaseKafkaClient("test-client", ["localhost:9092"], {
			defaultHeaders,
		});

		expect(client.getDefaultHeaders()).toEqual(defaultHeaders);
	});

	it("should merge default headers with message headers", async () => {
		const defaultHeaders = { "x-service-id": "test-service", "x-env": "prod" };
		const client = new BaseKafkaClient("test-client", ["localhost:9092"], {
			defaultHeaders,
		});

		(client as any).isConnected = true;
		const mockProducer = (client as any).producer;

		await client.sendMessages("test-topic", [
			{
				key: "key1",
				value: { data: "test" },
				headers: { "x-request-id": "123" },
			},
		]);

		const call = mockProducer.send.mock.calls[0][0];
		const sentHeaders = call.messages[0].headers;

		expect(sentHeaders).toEqual({
			"x-service-id": "test-service",
			"x-env": "prod",
			"x-request-id": "123",
		});
	});

	it("should allow message-level headers to override defaults", async () => {
		const defaultHeaders = { "x-service-id": "test-service", "x-env": "prod" };
		const client = new BaseKafkaClient("test-client", ["localhost:9092"], {
			defaultHeaders,
		});

		(client as any).isConnected = true;
		const mockProducer = (client as any).producer;

		await client.sendMessages("test-topic", [
			{
				key: "key1",
				value: { data: "test" },
				headers: { "x-env": "dev" }, // Override
			},
		]);

		const call = mockProducer.send.mock.calls[0][0];
		const sentHeaders = call.messages[0].headers;

		expect(sentHeaders["x-env"]).toBe("dev"); // Message-level wins
		expect(sentHeaders["x-service-id"]).toBe("test-service");
	});
});
