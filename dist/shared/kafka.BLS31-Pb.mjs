import { l as logger } from './kafka.Dv3r60xd.mjs';
import { JsonSerializer } from '../types.mjs';
import { K as KafkaError } from './kafka.D0nFqeDt.mjs';
import { nanoid } from 'nanoid';

class BaseProducer {
  producer;
  serializer;
  metrics;
  constructor(producer, metrics, serializer = new JsonSerializer()) {
    this.producer = producer;
    this.serializer = serializer;
    this.metrics = metrics || {
      publishSuccess: 0,
      publishFailures: 0,
      connectionRetries: 0,
      consumerConnections: 0
    };
  }
  /**
   * Publishes a single message to the configured Kafka topic.
   * It handles serialization and logs the outcome.
   * @param data The Kafka message object to publish.
   * @returns A promise that resolves when the message has been successfully published.
   * @throws {KafkaError} if the publish operation fails.
   */
  async publish(data) {
    try {
      logger.info(`Publishing to ${this.topic}`, { key: data.key });
      await this.producer.send({
        topic: this.topic,
        messages: [
          {
            key: data.key,
            value: this.serializer.serialize(data.value),
            headers: data.headers,
            timestamp: data.timestamp
          }
        ]
      });
      this.metrics.publishSuccess++;
      logger.debug(`Message published to ${this.topic}`);
    } catch (error) {
      this.metrics.publishFailures++;
      logger.error(`Publish failed to ${this.topic}`, error);
      throw new KafkaError(
        `Failed to publish to ${this.topic}`,
        error,
        {
          key: data.key
        }
      );
    }
  }
  /**
   * Publishes a batch of messages to the configured Kafka topic.
   * This is more efficient for sending multiple messages at once.
   * @param messages An array of Kafka messages to publish.
   * @returns A promise that resolves when the batch has been successfully published.
   * @throws {KafkaError} if the batch publish operation fails.
   */
  async publishBatch(messages) {
    try {
      await this.producer.send({
        topic: this.topic,
        messages: messages.map((msg) => ({
          key: msg.key,
          value: this.serializer.serialize(msg.value),
          headers: msg.headers,
          timestamp: msg.timestamp
        }))
      });
      this.metrics.publishSuccess += messages.length;
    } catch (error) {
      this.metrics.publishFailures += messages.length;
      throw new KafkaError(
        `Batch publish failed to ${this.topic}`,
        error,
        {
          messageCount: messages.length
        }
      );
    }
  }
}

const generateCorrelationId = (prefix) => {
  const id = nanoid(12);
  return `${prefix}_${id}` ;
};

class GenericKafkaProducer extends BaseProducer {
  topic;
  keyDeriver;
  constructor(producer, topic, keyDeriver) {
    super(producer);
    this.topic = topic;
    this.keyDeriver = keyDeriver;
  }
  generateKey(payload) {
    return this.keyDeriver(payload);
  }
  enrichAndFormatMessage(payload) {
    const key = this.generateKey(payload);
    const rawTimestamp = payload.timestamp;
    const timestamp = rawTimestamp instanceof Date ? rawTimestamp : new Date(String(rawTimestamp));
    const metadata = payload.metadata ?? {};
    const correlationId = metadata.correlationId;
    const schemaHash = metadata.schemaHash;
    const source = metadata.source;
    const headers = {
      "event-version": "1.0",
      "event-type": this.topic,
      timestamp: timestamp.toISOString(),
      ...correlationId ? { "correlation-id": String(correlationId) } : {},
      ...source ? { source: String(source) } : {},
      ...schemaHash ? { "schema-hash": String(schemaHash) } : {}
    };
    return {
      key,
      headers,
      value: payload
    };
  }
  stringifyHeaders(headers) {
    if (!headers) return void 0;
    const result = {};
    for (const [k, v] of Object.entries(headers)) {
      result[k] = v instanceof Date ? v.toISOString() : String(v);
    }
    return result;
  }
  async send(payload) {
    const enriched = this.enrichAndFormatMessage(payload);
    const stringifiedHeaders = this.stringifyHeaders(enriched.headers);
    await this.publish({
      key: enriched.key,
      value: enriched.value,
      headers: stringifiedHeaders
    });
    logger.info(`Published event to topic ${this.topic}`, {
      key: enriched.key,
      timestamp: enriched.value.timestamp?.toString()
    });
  }
}
function createEventPublisher(producer, topicName, schema, keyDeriver, source) {
  const producerInstance = new GenericKafkaProducer(
    producer,
    topicName,
    keyDeriver
  );
  return async (payload) => {
    const validated = schema.parse(payload);
    const event = {
      ...validated,
      metadata: {
        source,
        correlationId: generateCorrelationId("evt"),
        ...validated.metadata ?? {}
      }
    };
    await producerInstance.send(event);
  };
}

export { BaseProducer as B, createEventPublisher as c };
