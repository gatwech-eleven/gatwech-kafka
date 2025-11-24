import { KafkaClient } from "./kafka";
import type { KafkaClientOptions } from "../../types";

/**
 * Interface for a Kafka client extension.
 * Extensions can extend the client with additional functionality.
 */
export interface KafkaExtension<T = unknown, Name extends string = string> {
  name: Name;
  /**
   * Called when the client is created.
   * @param client The Kafka client instance
   * @returns The extension's public API
   */
  setup: (client: KafkaClient) => T;
}

/**
 * Branded type for Gatwech Kafka extension arrays.
 * Use this with `satisfies` for type-safe extension configuration.
 * 
 * @example
 * ```typescript
 * const extensions = [
 *   topicProvisioningExtension(topics),
 *   healthCheckExtension(),
 * ] satisfies GatwechKafkaExtensions;
 * ```
 */
export type GatwechKafkaExtensions = readonly KafkaExtension<any, string>[];

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
 * Type-safe configuration for Kafka client that incorporates default options.
 * Use with `satisfies` to ensure configuration correctness while preserving type inference.
 * 
 * All fields from {@link DEFAULT_OPTIONS} are optional and will be merged with defaults.
 * Only `clientId` and `brokers` are required.
 * 
 * @example
 * ```typescript
 * import { createKafkaClient, type KafkaClientConfig } from '@gatwech/kafka';
 * 
 * export const kafka = createKafkaClient({
 *   config: {
 *     clientId: 'my-service',
 *     brokers: ['localhost:9092'],
 *     // All other options are optional and use defaults from DEFAULT_OPTIONS
 *     topics: AppTopics,
 *     topicProvisioning: {
 *       partitions: 3,
 *       replicationFactor: 1,
 *     },
 *   },
 * } satisfies KafkaClientConfig);
 * ```
 * 
 * @see {@link DEFAULT_OPTIONS} for default values
 */
export type KafkaClientConfig<TExtensions extends KafkaExtension<any>[] = KafkaExtension<any>[]> = CreateKafkaClientConfig<TExtensions>;

/**
 * Helper type to extract the return type of an extension's setup function
 */
type ExtensionReturnType<T extends KafkaExtension<any>> = ReturnType<T["setup"]>;

/**
 * Helper type to map extension names to their return types
 */
type ExtensionsMap<TExtensions extends KafkaExtension<any>[]> = {
  [K in TExtensions[number] as K["name"]]: ExtensionReturnType<K>;
};

/**
 * The resulting client type, which is the base KafkaClient extended with extension functionality.
 */
export type ModularKafkaClient<TExtensions extends KafkaExtension<any>[]> =
  KafkaClient & {
    extensions: ExtensionsMap<TExtensions>;
  };

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
 * await client.extensions.topicProvisioning.provisionTopics();
 * await client.extensions.healthCheck.check();
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
  const extensionsMap: Record<string, any> = {};

  for (const extension of extensions) {
    if (extensionsMap[extension.name]) {
      throw new Error(
        `Extension name collision: '${extension.name}' is already registered.`
      );
    }
    extensionsMap[extension.name] = extension.setup(client);
  }

  // Return the extended client with namespaced extensions
  const extendedClient = client as unknown as ModularKafkaClient<TExtensions>;
  extendedClient.extensions = extensionsMap as ExtensionsMap<TExtensions>;

  return extendedClient;
}
