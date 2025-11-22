export class KafkaError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
    public readonly meta?: Record<string, unknown>
  ) {
    super(message);
    this.name = "KafkaError";
    Object.setPrototypeOf(this, KafkaError.prototype);
  }
}
