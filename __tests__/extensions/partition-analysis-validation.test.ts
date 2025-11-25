import { describe, it, expect } from 'vitest';
import { partitionAnalysisExtension } from '../../src/extensions/partition-analysis';
import { BaseKafkaClient } from '../../src/client/core/kafka';

describe('Partition Analysis Validation', () => {
    const mockClient = {} as BaseKafkaClient;
    const extension = partitionAnalysisExtension();
    const { checkPartitionSkew } = extension.setup(mockClient);

    it('should throw error if skewThreshold is <= 1.0', () => {
        expect(() => checkPartitionSkew([100, 100], 1.0)).toThrow('skewThreshold must be greater than 1.0');
        expect(() => checkPartitionSkew([100, 100], 0.5)).toThrow('skewThreshold must be greater than 1.0');
    });

    it('should throw error if distribution contains negative values', () => {
        expect(() => checkPartitionSkew([100, -10])).toThrow('Distribution cannot contain negative values');
    });
});
