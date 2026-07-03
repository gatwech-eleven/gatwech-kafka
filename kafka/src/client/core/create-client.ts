import { BaseKafkaClient } from "./kafka";
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
  setup: (client: BaseKafkaClient) => T;
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
 * Extract the TModule type from a single extension
 */
type ExtractModuleType<TExtension> = 
  TExtension extends KafkaExtension<infer TModule> ? TModule : {};

/**
 * Convert a union type to an intersection type.
 * This is the key to merging all extension methods into a single type.
 */
type UnionToIntersection<U> = 
  (U extends any ? (k: U) => void : never) extends ((k: infer I) => void) ? I : never;

/**
 * Merge all extracted modules into a single interface.
 * This complex utility type iterates over the array of extensions and safely merges
 * all their exposed methods into one final type.
 * 
 * Special handling for empty arrays: returns {} instead of never
 */
type MergeExtensions<TExtensions extends KafkaExtension<any>[]> = 
  TExtensions extends [] 
    ? {} 
    : TExtensions extends readonly [] 
      ? {}
      : UnionToIntersection<ExtractModuleType<TExtensions[number]>>;

/**
 * The Kafka client type with optional extension functionality.
 * Extension methods are flattened directly onto the client object for simpler usage.
 * This type explicitly preserves all BaseKafkaClient methods including their overload signatures.
 */
export type KafkaClient<TExtensions extends KafkaExtension<any>[] = []> = 
  BaseKafkaClient & MergeExtensions<TExtensions>;

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
 *     partitionAnalysisExtension(),
 *   ]
 * });
 *
 * // Access base client methods
 * await client.connect();
 *
 * // Access extension methods (flattened directly on client)
 * await client.analyzePartitionSkew(...);
 * const optimal = client.calculateOptimalPartitions(...);
 * ```
 */
// Overload: When extensions are explicitly provided
export function createKafkaClient<TExtensions extends KafkaExtension<any>[]>(
  options: CreateKafkaClientConfig<TExtensions> & { extensions: TExtensions }
): KafkaClient<TExtensions>;

// Overload: When no extensions property is provided (defaults to empty array)
export function createKafkaClient(
  options: Omit<CreateKafkaClientConfig<[]>, 'extensions'>
): KafkaClient<[]>;

// Implementation
export function createKafkaClient<TExtensions extends KafkaExtension<any>[]>(
  options: CreateKafkaClientConfig<TExtensions>
): KafkaClient<TExtensions> {
  const { config, extensions = [] } = options;
  const { clientId, brokers, ...clientOptions } = config;

  // Initialize the base client
  const client = new BaseKafkaClient(clientId, brokers, clientOptions);

  // Apply extensions - flatten methods directly onto client
  const flattenedExtensions: Record<string, any> = {};
  const extensionNames = new Set<string>();

  for (const extension of extensions) {
    // Check for name collisions
    if (extensionNames.has(extension.name)) {
      throw new Error(
        `Extension name collision: '${extension.name}' is already registered.`
      );
    }
    extensionNames.add(extension.name);

    // Get extension methods and flatten them onto the client
    const methods = extension.setup(client);
    Object.assign(flattenedExtensions, methods);
  }

  // Return the extended client with flattened extension methods
  return Object.assign(client, flattenedExtensions) as unknown as KafkaClient<TExtensions>;
}
