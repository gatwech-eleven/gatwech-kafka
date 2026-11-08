import type { BaseKafkaClient } from "../client/core/kafka";
import type { KafkaExtension } from "../client/core/create-client";
import { logger } from "../utils/logger";

const DEFAULT_SKEW_THRESHOLD = 1.5;

/** Defines the throughput metrics needed for the 4-Pillar calculation. */
export interface ThroughputMetrics {
	/** Target total throughput (T) in MB/s. */
	targetThroughputMBps: number;
	/** Producer throughput per partition (P) in MB/s. */
	producerPerPartitionMBps: number;
	/** Consumer throughput per partition (C) in MB/s (slower part). */
	consumerPerPartitionMBps: number;
	/** Desired consumer parallelism (max active instances). */
	desiredParallelism: number;
	/** Projected growth factor (e.g., 2.0 for 100% growth). */
	growthFactor: number;
}

export interface PartitionSkewResult {
	status: "BALANCED" | "SKEWED";
	hotPartitions: string[];
}

/**
 * Calculates the optimal number of partitions based on the 4-Pillar Framework
 * derived from the video: Throughput, Latency, Consumer Parallelism, and Future Growth.
 *
 * @throws Error if any metric is negative.
 */
export const calculateOptimalPartitions = (
	metrics: ThroughputMetrics,
): number => {
	const {
		targetThroughputMBps: T,
		producerPerPartitionMBps: P,
		consumerPerPartitionMBps: C,
		desiredParallelism,
		growthFactor,
	} = metrics;

	if (T < 0 || P < 0 || C < 0 || desiredParallelism < 0 || growthFactor < 0) {
		throw new Error("Throughput metrics cannot be negative.");
	}

	// Pillar 1: Throughput-Based Minimum
	// Formula: Max(T / P, T / C)
	// Avoid division by zero
	const producerMin = P > 0 ? T / P : Infinity;
	const consumerMin = C > 0 ? T / C : Infinity;

	// If both are 0 (invalid config usually), default to 1 to avoid issues, or let Infinity propagate if T > 0
	let throughputBasedMin = 0;
	if (P === 0 && C === 0 && T > 0) {
		throughputBasedMin = Infinity; // Impossible to satisfy
	} else if (P === 0 && C === 0 && T === 0) {
		throughputBasedMin = 0;
	} else {
		throughputBasedMin = Math.ceil(Math.max(producerMin, consumerMin));
	}

	if (!isFinite(throughputBasedMin)) {
		// Fallback or throw? For now, if P or C is 0 and T > 0, it implies infinite partitions needed.
		// Let's assume P and C should be > 0 for valid calculation.
		// But to be safe against div by zero:
		if (T > 0 && (P <= 0 || C <= 0)) {
			// If we can't process anything, we need infinite partitions?
			// Realistically, return a safe minimum or throw.
			// Let's stick to the logic: if P or C is 0, we can't handle load.
			// For this exercise, let's assume valid inputs > 0 usually.
			// If T is 0, we need 0 partitions (but min is 1).
		}
	}

	// Pillar 2: Latency (Handled by taking the max of the bottleneck component - C)
	// Pillar 3: Consumer Parallelism
	const parallelismTarget = desiredParallelism;

	// Pillar 4: Future Growth (Apply growth factor to the greatest required number)
	const finalRequiredPartitions = Math.max(
		throughputBasedMin,
		parallelismTarget,
	);

	// Always round up to ensure enough capacity for the scaled throughput
	const finalPartitionCount = Math.ceil(finalRequiredPartitions * growthFactor);

	// Best Practice: Ensure the count is never 0 or negative.
	return Math.max(1, finalPartitionCount);
};

/**
 * CLIENT EXTENSION: Adds partition analysis and calculation utilities to the Kafka Client.
 */
export const partitionAnalysisExtension = (): KafkaExtension<
	{
		calculatePartitions: typeof calculateOptimalPartitions;
		checkPartitionSkew: (
			distribution: number[],
			skewThreshold?: number,
		) => PartitionSkewResult;
	},
	"partitionAnalysis"
> => ({
	name: "partitionAnalysis",
	setup: (client: BaseKafkaClient) => {
		logger.info(`[Extension] Installing partition analysis client...`);

		return {
			// Expose the core calculation method
			calculatePartitions: calculateOptimalPartitions,

			// Expose a helper to validate partitioning key distribution (Hot Partition check)
			checkPartitionSkew: (
				distribution: number[],
				skewThreshold: number = DEFAULT_SKEW_THRESHOLD,
			): PartitionSkewResult => {
				if (skewThreshold <= 1.0) {
					throw new Error("skewThreshold must be greater than 1.0");
				}
				if (distribution.some((v) => v < 0)) {
					throw new Error("Distribution cannot contain negative values");
				}

				if (distribution.length === 0) {
					return { status: "BALANCED", hotPartitions: [] };
				}

				const totalLoad = distribution.reduce((a, b) => a + b, 0);
				const avgLoad = totalLoad / distribution.length;

				const hotPartitions = distribution
					.map((load, index) => {
						if (load >= avgLoad * skewThreshold) {
							return `Partition ${index} is 'Hot' (Load: ${load.toFixed(2)}, Avg: ${avgLoad.toFixed(2)})`;
						}
						return null;
					})
					.filter((item): item is string => item !== null);

				if (hotPartitions.length > 0) {
					logger.warn(
						`[PartitionSkew] Detected ${hotPartitions.length} hot partition(s).`,
					);
					return { status: "SKEWED", hotPartitions };
				}

				return { status: "BALANCED", hotPartitions: [] };
			},
		};
	},
});
