import { logger } from "../../utils/logger";
import type { Kafka, ConsumerConfig } from "kafkajs";
import type { StrictOmit } from "../../types/utility-types";
import type { KafkaClientOptions, KafkaMetrics } from "../../types";

type KafkaConsumerInit = ConsumerConfig & { groupInstanceId?: string };

/**
 * @deprecated Use `consumeWithRetryAndDLQ` instead for better type safety and DLQ support.
 */
export async function createConsumerForClient(
  kafka: Kafka,
  clientOptions: Required<KafkaClientOptions>,
  groupId: string,
  options: ConsumerConfig & { groupInstanceId?: string },
  consumersMap: Map<string, ReturnType<Kafka["consumer"]>>,
  metrics: KafkaMetrics
): Promise<ReturnType<Kafka["consumer"]>> {
  const {
    groupId: _maybe,
    retry: _retry,
    sessionTimeout,
    heartbeatInterval,
    rebalanceTimeout,
    groupInstanceId,
    ...restOptions
  } = options;

  if (!groupId) {
    throw new Error("groupId is required");
  }

  const consumerOptions: KafkaConsumerInit = {
    groupId,
    ...(restOptions as StrictOmit<ConsumerConfig, "groupId" | "retry">),
    sessionTimeout: sessionTimeout ?? clientOptions.sessionTimeout,
    heartbeatInterval: heartbeatInterval ?? clientOptions.heartbeatInterval,
    rebalanceTimeout: rebalanceTimeout ?? clientOptions.rebalanceTimeout,
    groupInstanceId: groupInstanceId ?? undefined,
    retry: {
      maxRetryTime: 30000,
      initialRetryTime: 1000,
      factor: 2,
      multiplier: 2,
      retries: 5,
      ...(options.retry || {}),
    },
  };

  const consumer = kafka.consumer(consumerOptions);

  consumersMap.set(groupId, consumer);
  metrics.consumerConnections++;

  consumer.on("consumer.connect", () => {
    logger.info("Kafka consumer connected", { groupId });
  });

  consumer.on("consumer.disconnect", () => {
    logger.info("Kafka consumer disconnected", { groupId });
    metrics.consumerConnections--;
  });

  consumer.on("consumer.network.request_timeout", (payload) => {
    logger.warn("Kafka consumer network request timeout", {
      groupId,
      payload,
    });
  });

  consumer.on("consumer.crash", (err) => {
    logger.error("Kafka consumer crash", { groupId, error: err });

    const tracked = consumersMap.get(groupId);
    if (!tracked) return;

    let retries = 0;

    const attempt = async () => {
      if (!consumersMap.has(groupId)) return;
      try {
        logger.info("Attempting to reconnect Kafka consumer", {
          groupId,
          attempt: retries + 1,
        });
        await tracked.connect();
        logger.info("Kafka consumer reconnected", { groupId });
      } catch (e) {
        retries++;
        metrics.connectionRetries++;
        const delay = Math.min(1000 * 2 ** retries, 30_000);
        logger.warn("Kafka consumer reconnect failed, scheduling retry", {
          groupId,
          retries,
          delay,
          error: e,
        });
        setTimeout(attempt, delay);
      }
    };

    setTimeout(attempt, 1000);
  });

  consumer.on("consumer.group_join", (info) => {
    logger.info("Consumer group join", { groupId, info });
  });

  return consumer;
}
