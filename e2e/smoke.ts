import { nanoid } from "nanoid";
import { KafkaClient } from "../src/client/kafka.ts";
import { ensureTopicsWithConfig } from "../src/config/ensure-topics.ts";

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = `smoke-${Date.now()}`;
  const groupId = `smoke-${nanoid().slice(0, 6)}`;

  console.log(`🚭 Smoke: bootstrap=${bootstrap}, topic=${topic}`);
  const client = new KafkaClient("smoke-client", [bootstrap], {
    metricsEnabled: false,
    connectWithRetries: true,
    // CI brokers may take longer to become ready; allow up to 3 minutes
    maxConnectAttemptTimeMs: 180_000,
    connectPollIntervalMs: 1000,
  });
  await client.connect();

  const admin = client.getAdmin();
  await ensureTopicsWithConfig(admin, {
    includeBuiltIn: false,
    partitions: 1,
    replicationFactor: 1,
    additionalTopics: [
      {
        name: topic,
        config: {
          name: topic,
          retentionMs: 3600000,
          cleanupPolicy: "delete",
        } as any,
      },
    ],
  });

  const consumer = await client.createConsumer(groupId, { groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const got = new Promise<void>((resolve, reject) => {
    const to = setTimeout(() => reject(new Error("Smoke timeout")), 10000);
    consumer
      .run({
        eachMessage: async ({ message }) => {
          console.log(
            "✅ Smoke consumed",
            message.key?.toString(),
            message.value?.toString()
          );
          clearTimeout(to);
          resolve();
        },
      })
      .catch(reject);
  });

  const producer = client.getProducer();
  await producer.send({
    topic,
    messages: [{ key: "k", value: JSON.stringify({ ok: true }) }],
  });
  console.log("🚀 Smoke published");

  await got;
  await consumer.disconnect();
  await client.disconnect();
}

main().catch((err) => {
  console.error("❌ Smoke Failed:", err);
  process.exit(1);
});
