import { describe, it, expect, vi } from 'vitest';
import { calculateOptimalPartitions, partitionAnalysisExtension, type ThroughputMetrics } from '../../src/extensions/partition-analysis';
import { KafkaClient } from '../../src/client/core/kafka';

describe('Partition Strategy Test Suite', () => {
    describe('I. Unit Tests (Partition Calculation Logic)', () => {
        it('UT-TP-001: Throughput Bottleneck (Consumer)', () => {
            const metrics: ThroughputMetrics = {
                targetThroughputMBps: 200,
                producerPerPartitionMBps: 20,
                consumerPerPartitionMBps: 10,
                desiredParallelism: 5,
                growthFactor: 1.0
            };
            // max(200/20=10, 200/10=20) = 20
            expect(calculateOptimalPartitions(metrics)).toBe(20);
        });

        it('UT-TP-002: Throughput Bottleneck (Producer)', () => {
            const metrics: ThroughputMetrics = {
                targetThroughputMBps: 200,
                producerPerPartitionMBps: 5,
                consumerPerPartitionMBps: 20,
                desiredParallelism: 10,
                growthFactor: 1.0
            };
            // max(200/5=40, 200/20=10) = 40
            expect(calculateOptimalPartitions(metrics)).toBe(40);
        });

        it('UT-PL-003: Parallelism Override', () => {
            const metrics: ThroughputMetrics = {
                targetThroughputMBps: 200,
                producerPerPartitionMBps: 20,
                consumerPerPartitionMBps: 10,
                desiredParallelism: 30, // Higher than throughput requirement (20)
                growthFactor: 1.0
            };
            // max(20, 30) = 30
            expect(calculateOptimalPartitions(metrics)).toBe(30);
        });

        it('UT-GR-004: Future Growth (50%)', () => {
            const metrics: ThroughputMetrics = {
                targetThroughputMBps: 100,
                producerPerPartitionMBps: 20,
                consumerPerPartitionMBps: 10,
                desiredParallelism: 10,
                growthFactor: 1.5
            };
            // Base: max(100/20=5, 100/10=10, 10) = 10
            // Growth: 10 * 1.5 = 15
            expect(calculateOptimalPartitions(metrics)).toBe(15);
        });

        it('UT-ZP-005: Zero/Edge Case', () => {
            const metrics: ThroughputMetrics = {
                targetThroughputMBps: 0,
                producerPerPartitionMBps: 1,
                consumerPerPartitionMBps: 1,
                desiredParallelism: 1,
                growthFactor: 1.0
            };
            // Min partition count is 1
            expect(calculateOptimalPartitions(metrics)).toBe(1);
        });

        it('should throw error for negative inputs', () => {
             const metrics: ThroughputMetrics = {
                targetThroughputMBps: -100,
                producerPerPartitionMBps: 20,
                consumerPerPartitionMBps: 10,
                desiredParallelism: 10,
                growthFactor: 1.0
            };
            expect(() => calculateOptimalPartitions(metrics)).toThrow("Throughput metrics cannot be negative.");
        });
    });

    describe('II. End-to-End (E2E) Test Scenarios (Skew Validation)', () => {
        // Setup the extension
        const mockClient = {} as KafkaClient;
        const extension = partitionAnalysisExtension();
        const installed = extension.setup(mockClient);
        const { checkPartitionSkew } = installed;

        it('E2E-SK-001: Perfect Balance', () => {
            const distribution = [100, 100, 100, 100];
            const result = checkPartitionSkew(distribution);
            expect(result.status).toBe('BALANCED');
            expect(result.hotPartitions).toHaveLength(0);
        });

        it('E2E-SK-002: Moderate Skew (<1.5x)', () => {
            // Avg: 112.5. Threshold: 112.5 * 1.5 = 168.75
            // 150 < 168.75 -> Balanced
            const distribution = [100, 100, 150, 100];
            const result = checkPartitionSkew(distribution);
            expect(result.status).toBe('BALANCED');
        });

        it('E2E-SK-003: Severe Hot Partition', () => {
            // Avg: 87.5. Threshold: 87.5 * 1.5 = 131.25
            // 200 > 131.25 -> Skewed
            const distribution = [50, 50, 200, 50];
            const result = checkPartitionSkew(distribution);
            expect(result.status).toBe('SKEWED');
            expect(result.hotPartitions).toHaveLength(1);
            expect(result.hotPartitions[0]).toContain("Partition 2 is 'Hot'");
        });

        it('E2E-SK-004: Multiple Hot Partitions', () => {
            // Avg: 200. Threshold: 300
            // 300 >= 300? Code uses >. Let's check logic.
            // If logic is > avg * threshold:
            // 300 > 200 * 1.5 (300) -> False.
            // Wait, the requirement says "300 >= 1.5x avg threshold" in the table?
            // "300 >= 1.5x avg threshold" implies inclusive.
            // My code implemented `load > avgLoad * skewThreshold`.
            // Let's verify the test expectation vs code.
            // If I want to match the table exactly, I might need to adjust code or test values.
            // Let's use values that clearly exceed to be safe, or check boundary if critical.
            // Table says: [100, 300, 100, 300]. Avg = 200. 1.5x = 300.
            // If code is `> 300`, then 300 is NOT hot.
            // If code is `>= 300`, then 300 IS hot.
            // Let's assume strict inequality for "Hot" usually means "significantly higher".
            // But let's check the code I wrote: `if (load > avgLoad * skewThreshold)`
            // So [100, 300, 100, 300] would be BALANCED with threshold 1.5.
            // To make it SKEWED as per table expectation, I should probably use 301 or change logic to `>=`.
            // Or maybe the table implies "Severe" skew.
            // Let's stick to the code logic I implemented (`>`) and adjust test values to clearly trigger it, 
            // OR update code to `>=` if that was the user intent.
            // The user table says "300 >= 1.5x avg threshold" in the expected result column.
            // So I should probably change the code to `>=` to match the user's expectation in the table.
            
            // Let's update the test to expect what the CURRENT code does, 
            // or better, I'll update the code in the next step if I see it failing.
            // For now, I'll write the test with values that SHOULD pass with `>` logic if I bump it slightly,
            // OR I will respect the table and update the code.
            // Actually, I'll update the code to `>=` in a separate step if needed.
            // For this file creation, I'll use values that definitely trigger `>`:
            // [100, 310, 100, 310] -> Avg 205. 1.5x = 307.5. 310 > 307.5.
            
            // Wait, let's look at the table again.
            // "E2E-SK-004 ... [100, 300, 100, 300] ... SKEWED (300 >= 1.5x avg threshold)"
            // It seems the user expects 300 to be flagged.
            // I will update the test to use the user's values, and if it fails, I will fix the code to use `>=`.
            
            const distribution = [100, 300, 100, 300];
            const result = checkPartitionSkew(distribution);
            
            // NOTE: This might fail if code is `>`. I will fix code if so.
            // Actually, I can just fix the code now? No, I'm writing the test file.
            // I'll write the test expecting SKEWED.
            expect(result.status).toBe('SKEWED');
            expect(result.hotPartitions).toHaveLength(2);
        });
        
        it('should allow custom skew threshold', () => {
             const distribution = [100, 120, 100, 100];
             // Avg 105. Default 1.5x = 157.5. 120 is fine.
             // Custom threshold 1.1x = 115.5. 120 > 115.5 -> Hot.
             const result = checkPartitionSkew(distribution, 1.1);
             expect(result.status).toBe('SKEWED');
             expect(result.hotPartitions[0]).toContain("Partition 1 is 'Hot'");
        });
    });
});
