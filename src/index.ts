export * from "./client";
export * from "./config";
export * from "./types";
export * as validation from "./validation";
export * as producer from "./producer/validate-and-send";
export * as schemas from "./schemas";
export * from "./utils/brokers";

export type { Producer } from "kafkajs";

export { createEventPublisher } from "./client/event.factory";
export { BaseProducerSchema } from "./schemas/base.schema";
