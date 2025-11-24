import { nanoid } from "nanoid";
import supertest from "supertest";
import { z } from "@gatwech/utils/zod";
import express, { type Request, type Response } from "express";
import { KafkaClient } from '../src/client/core/kafka.ts';
import { createServicePublisherFactory } from '../src/client/producers/service-publisher-factory.ts';
import { initializeKafkaTopics } from "../src/config/ensure-topics.ts";
import { UserRegisteredSchema } from "../src/schemas/user-schemas.ts";
import { consumeWithRetryAndDLQ } from '../src/client/consumers/consume-with-dlq.ts';
import type { TopicConfig } from "../src/config/topics/topic-config-schema.ts";

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || "localhost:9092";
  const topic = process.env.EXPRESS_INTEGRATION_TOPIC || "user.registered";
  const authSource = process.env.AUTH_SOURCE || "auth-express-e2e";

  console.log(`🧪 Express Integration: bootstrap=${bootstrap}, topic=${topic}`);

  // Shared: ensure topic exists
  const adminClient = new KafkaClient("admin-express-e2e", [bootstrap], {
    metricsEnabled: false,
    connectWithRetries: true,
  });
  await adminClient.connect();
  try {
    await initializeKafkaTopics(adminClient.getAdmin(), {
      partitions: 3,
      replicationFactor: 1,
      waitForLeadersMs: 15000,
      additionalTopics: [
        {
          name: topic,
          config: {
            name: topic,
            retentionMs: 86400000,
            cleanupPolicy: "delete",
          } as any,
        },
        {
          name: `${topic}.dlq`,
          config: {
            name: `${topic}.dlq`,
            retentionMs: 86400000,
            cleanupPolicy: "delete",
          } as any,
        },
      ],
    });
  } finally {
    await adminClient.disconnect();
  }

  // Notification service (consumer) + app
  const notificationClient = new KafkaClient(
    "notification-express-e2e",
    [bootstrap],
    { metricsEnabled: false, connectWithRetries: true }
  );
  await notificationClient.connect();
  const groupId = `notification-express-${nanoid().slice(0, 6)}`;
  let lastProcessed: any = null;
  let lastDlq: any = null;

  const topicConfig: TopicConfig = {
    name: topic,
    retentionMs: 86400000,
    cleanupPolicy: "delete",
    schema: UserRegisteredSchema,
  } as any;

  // Main consumer with retry + DLQ
  const consumer = await consumeWithRetryAndDLQ(
    notificationClient,
    groupId,
    topicConfig,
    async ({ message }) => {
      const raw = message.value?.toString() || "{}";
      const json = JSON.parse(raw);
      // Simulate failure for DLQ when requested
      if (
        json?.name === "FAIL" ||
        String(json?.email).startsWith("express-dlq-")
      ) {
        throw new Error("simulated failure for DLQ");
      }
      lastProcessed = json;
    },
    { dlqEnabled: true, maxRetries: 2, retryBackoffMs: 200 }
  );

  // DLQ consumer to assert DLQ publication
  const dlqGroupId = `${groupId}-dlq`;
  const dlqConsumer = await notificationClient.createConsumer(dlqGroupId, {
    groupId: dlqGroupId,
  });
  await dlqConsumer.connect();
  await dlqConsumer.subscribe({ topic: `${topic}.dlq`, fromBeginning: true });
  dlqConsumer
    .run({
      eachMessage: async ({ message }) => {
        try {
          const raw = message.value?.toString() || "{}";
          lastDlq = JSON.parse(raw);
        } catch {}
      },
    })
    .catch((e) => console.error("DLQ consumer error", e));

  const notificationApp = express();
  notificationApp.get("/health", (_req: Request, res: Response) =>
    res.json({ ok: true })
  );
  notificationApp.get("/events/last", (_req: Request, res: Response) =>
    res.json({ lastProcessed })
  );

  const authClient = new KafkaClient("auth-express-e2e", [bootstrap], {
    metricsEnabled: false,
    connectWithRetries: true,
  });
  await authClient.connect();
  const producer = authClient.getProducer();
  const defineEvent = createServicePublisherFactory(producer, authSource);
  const getPartitionKey = (p: z.infer<typeof UserRegisteredSchema>) => p.email;
  const publishUserRegistered = defineEvent(
    topic,
    UserRegisteredSchema,
    getPartitionKey
  );

  const authApp = express();
  authApp.use(express.json());
  authApp.post("/signup", async (req: Request, res: Response) => {
    try {
      const body = (req.body as z.infer<typeof UserRegisteredSchema>) || {};
      // Validate and publish using service publisher
      await publishUserRegistered({
        id: body.id || nanoid(),
        email: body.email,
        name: body.name,
      } as z.infer<typeof UserRegisteredSchema>);
      res.status(202).json({ status: "accepted" });
    } catch (e: any) {
      res.status(400).json({ error: e?.message || String(e) });
    }
  });

  // Use supertest to call the auth app without binding ports
  const authRequest = supertest(authApp);
  const notificationRequest = supertest(notificationApp);

  const email = `express-${nanoid().slice(0, 4)}@example.com`;
  console.log("🚀 POST /signup", { email });
  const resp = await authRequest
    .post("/signup")
    .send({ email, name: "Express User" });
  if (resp.status !== 202)
    throw new Error(`Signup failed: ${resp.status} ${resp.text}`);

  // Poll the notification app to confirm processing
  const start = Date.now();
  const timeoutMs = 20000;
  while (Date.now() - start < timeoutMs) {
    const got = await notificationRequest.get("/events/last");
    if (got.body?.lastProcessed?.email === email) {
      console.log("🎉 Express Integration Success:", got.body.lastProcessed);
      break;
    }
    await sleep(500);
  }
  if (!lastProcessed || lastProcessed.email !== email) {
    throw new Error("Timeout waiting for notification processing");
  }

  // Now send a message that should be DLQ'd
  const dlqEmail = `express-dlq-${nanoid().slice(0, 4)}@example.com`;
  console.log("🚀 POST /signup (DLQ)", { email: dlqEmail });
  const resp2 = await authRequest
    .post("/signup")
    .send({ email: dlqEmail, name: "FAIL" });
  if (resp2.status !== 202)
    throw new Error(`Signup (DLQ) failed: ${resp2.status} ${resp2.text}`);

  const dlqStart = Date.now();
  while (Date.now() - dlqStart < timeoutMs) {
    if (lastDlq && lastDlq.originalValue?.email === dlqEmail) {
      console.log("🧰 DLQ observed:", lastDlq);
      break;
    }
    await sleep(500);
  }
  if (!lastDlq || lastDlq.originalValue?.email !== dlqEmail) {
    throw new Error("Timeout waiting for DLQ message");
  }

  // Cleanup
  await consumer.disconnect();
  await dlqConsumer.disconnect();
  await notificationClient.disconnect();
  await authClient.disconnect();
}

main().catch((err) => {
  console.error("❌ Express Integration Failed:", err);
  process.exit(1);
});
