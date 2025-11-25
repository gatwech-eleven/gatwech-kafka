/**
 * Extensions module
 * Provides optional extensions for the Kafka client
 */

// ============================================================================
// Partition Analysis Extension
// ============================================================================

export { partitionAnalysisExtension, calculateOptimalPartitions } from "./partition-analysis";

export type {
  PartitionSkewResult,
  ThroughputMetrics,
} from "./partition-analysis";

// ============================================================================
// Consumer Extension
// ============================================================================

export { consumerExtension } from "./consumer-extension";

export type {
  ConsumerExtensionAPI,
} from "./consumer-extension";
