import { BaseKafkaClient } from '../../src/client/core/kafka';
import { GenericKafkaProducer } from '../../src/client/producers/generic-producer';
import { z } from "@gatwech/utils/zod";
import { nanoid } from "nanoid";
import crypto from "node:crypto";

// Simple event schema (epoch timestamp enforced externally by producer logic)
const BenchEventSchema = z.object({
  id: z.string(),
  payload: z.string(),
  timestamp: z.number().int().nonnegative(),
  metadata: z
    .object({
      correlationId: z.string().optional(),
      source: z.string().optional(),
    })
    .optional(),
});

type BenchEvent = z.infer<typeof BenchEventSchema>;

function hrtimeMs(): number {
  const [s, ns] = process.hrtime();
  return s * 1000 + ns / 1e6;
}

async function ensureTopic(client: BaseKafkaClient, topic: string, partitions = 3) {
  try {
    const admin = client.getAdmin();
    await admin.connect();
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
      waitForLeaders: true,
    });
    await admin.disconnect();
    // eslint-disable-next-line no-console
    console.log(`Topic ensured: ${topic}`);
  } catch (e: any) {
    // eslint-disable-next-line no-console
    console.warn(`Topic creation skipped: ${e?.message || e}`);
  }
}

function buildEvents(count: number): BenchEvent[] {
  const arr: BenchEvent[] = [];
  for (let i = 0; i < count; i++) {
    arr.push({
      id: nanoid(),
      payload: `event-${i}-${Math.random().toString(36).slice(2)}`,
      timestamp: Date.now(),
      metadata: { source: "bench" },
    });
  }
  return arr;
}

function formatNumber(n: number): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(n);
}

async function runBenchmark() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topicBase = process.env.BENCH_TOPIC || `bench-${Date.now()}`;
  const iterations = Number(process.env.BENCH_MESSAGES || 5000);
  const batchSize = Number(process.env.BENCH_BATCH_SIZE || 100);
  const client = new BaseKafkaClient("bench-client", [bootstrap], {
    metricsEnabled: false,
  });
  await client.connect();

  await ensureTopic(client, topicBase, 6);

  const producer = client.getProducer();
  const generic = new GenericKafkaProducer<typeof BenchEventSchema>(
    producer,
    topicBase,
    // key derivation fallback – stable hash of id for partition impact
    (p) =>
      crypto
        .createHash("sha1")
        .update(String((p as any).id))
        .digest("hex")
  );

  // Build all events upfront to minimize overhead differences
  const events = buildEvents(iterations);

  // SINGLE SEND MODE
  const singleStart = hrtimeMs();
  for (const ev of events) {
    // schema parse (simulate same validation overhead)
    BenchEventSchema.parse(ev);
    await generic.send(ev as any);
  }
  const singleElapsed = hrtimeMs() - singleStart;

  // BATCH SEND MODE
  // Rebuild events with fresh timestamps to avoid duplicate keys/time drift confusion
  const batchEvents = buildEvents(iterations);
  const batchItems = batchEvents.map((e) => ({ key: e.id, payload: e }));
  const batchStart = hrtimeMs();

  // Chunk into batches
  for (let i = 0; i < batchItems.length; i += batchSize) {
    const slice = batchItems.slice(i, i + batchSize);
    // parse each (simulate same validation)
    for (const itm of slice) BenchEventSchema.parse(itm.payload);
    await generic.sendBatch(slice as any);
  }
  const batchElapsed = hrtimeMs() - batchStart;

  const singleRate = iterations / (singleElapsed / 1000);
  const batchRate = iterations / (batchElapsed / 1000);
  const improvement = ((batchRate - singleRate) / singleRate) * 100;

  // eslint-disable-next-line no-console
  console.log("\n==== Kafka Publish Performance Benchmark ====");
  console.log(`Broker: ${bootstrap}`);
  console.log(`Topic: ${topicBase}`);
  console.log(`Messages: ${iterations}`);
  console.log(`Batch size: ${batchSize}`);
  console.log("-- Results --");
  console.log(`Single-send elapsed: ${formatNumber(singleElapsed)} ms`);
  console.log(`Single-send rate:   ${formatNumber(singleRate)} msg/sec`);
  console.log(`Batch-send elapsed: ${formatNumber(batchElapsed)} ms`);
  console.log(`Batch-send rate:    ${formatNumber(batchRate)} msg/sec`);
  console.log(
    `Relative improvement (batch vs single): ${formatNumber(improvement)}%`
  );

  await client.disconnect();
}

runBenchmark().catch((e) => {
  console.error("Benchmark failed", e);
  process.exit(1);
});
