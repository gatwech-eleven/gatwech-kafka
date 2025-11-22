import { KafkaClient } from "./kafka";
import type { KafkaClientOptions } from "../types";

/**
 * Interface for a Kafka client extension.
 * Extensions can extend the client with additional functionality.
 */
export interface KafkaExtension<T = unknown> {
  name: string;
  /**
   * Called when the client is created.
   * @param client The Kafka client instance
   * @returns The extension's public API
   */
  setup: (client: KafkaClient) => T;
}

/**
 * Configuration for creating a modular Kafka client.
 */
export interface CreateKafkaClientConfig<
  TExtensions extends KafkaExtension<any>[] = KafkaExtension<any>[]
> {
  /**
   * Kafka client configuration options
   */
  config: {
    clientId: string;
    brokers: string[];
  } & KafkaClientOptions;

  /**
   * List of extensions to register
   */
  extensions?: TExtensions;
}

/**
 * Helper type to extract the return type of an extension's setup function
 */
type ExtensionReturnType<T extends KafkaExtension<any>> = ReturnType<T["setup"]>;

/**
 * Helper type to merge extension return types into a single object
 */
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (
  k: infer I
) => void
  ? I
  : never;

/**
 * The resulting client type, which is the base KafkaClient extended with extension functionality.
 */
export type ModularKafkaClient<TExtensions extends KafkaExtension<any>[]> =
  KafkaClient &
    UnionToIntersection<
      TExtensions extends Array<infer P>
        ? P extends KafkaExtension<any>
          ? ExtensionReturnType<P>
          : never
        : never
    >;

/**
 * Create a modular Kafka client with extensions.
 *
 * @example
 * ```typescript
 * const client = createKafkaClient({
 *   config: {
 *     clientId: 'my-service',
 *     brokers: ['localhost:9092'],
 *   },
 *   extensions: [
 *     topicProvisioningExtension(topics),
 *     healthCheckExtension(),
 *   ]
 * });
 *
 * // Access base client methods
 * await client.connect();
 *
 * // Access extension methods
 * await client.provisionTopics();
 * await client.healthCheck();
 * ```
 */
export function createKafkaClient<TExtensions extends KafkaExtension<any>[]>(
  options: CreateKafkaClientConfig<TExtensions>
): ModularKafkaClient<TExtensions> {
  const { config, extensions = [] } = options;
  const { clientId, brokers, ...clientOptions } = config;

  // Initialize the base client
  const client = new KafkaClient(clientId, brokers, clientOptions);

  // Apply extensions
  const clientExtensions = extensions.reduce((acc, extension) => {
    const ext = extension.setup(client);
    return { ...acc, ...ext };
  }, {});

  // Return the extended client
  // We use a proxy or Object.assign to mix in the extension methods
  return Object.assign(client, clientExtensions) as ModularKafkaClient<TExtensions>;
}
