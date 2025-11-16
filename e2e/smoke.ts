import { nanoid } from "nanoid";
import { KafkaClient } from "../src/client/kafka.ts";
import { ensureTopicsWithConfig } from "../src/config/ensure-topics.ts";

async function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForBroker(client: KafkaClient, timeoutMs = 20000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const admin = client.getAdmin();
      await admin.connect();
      const info = await admin.describeCluster();
      await admin.disconnect();
      if (info.controller) return;
    } catch {}
    await wait(500);
  }
  throw new Error("Broker readiness timeout");
}

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = `smoke-${Date.now()}`;
  const groupId = `smoke-${nanoid().slice(0, 6)}`;

  console.log(`🚭 Smoke: bootstrap=${bootstrap}, topic=${topic}`);
  const client = new KafkaClient("smoke-client", [bootstrap], {
    metricsEnabled: false,
  });
  await client.connect();
  await waitForBroker(client);

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
