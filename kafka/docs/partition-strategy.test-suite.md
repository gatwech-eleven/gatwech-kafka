# 🧪 Kafka Partition Analysis Test Suite

This test suite validates both the **partitionAnalysisClient** logic and the **4-Pillar calculation strategy**.

---

## I. Unit Tests (Partition Calculation Logic)

Tests for `calculateOptimalPartitions` based on the 4-Pillar Framework.

### **Unit Test Table**

| Test ID       | Scenario                         | Input (T, P, C, P-Target, Growth) | Expected | Rationale                                          |
| ------------- | -------------------------------- | --------------------------------- | -------- | -------------------------------------------------- |
| **UT-TP-001** | Throughput Bottleneck (Consumer) | 200, 20, 10, 5, 1.0               | **20**   | max(200/20=10, 200/10=20). Consumer is bottleneck. |
| **UT-TP-002** | Throughput Bottleneck (Producer) | 200, 5, 20, 10, 1.0               | **40**   | max(40, 10). Producer is bottleneck.               |
| **UT-PL-003** | Parallelism Override             | 200, 20, 10, 30, 1.0              | **30**   | Pillar 3 requires ≥ 30 partitions.                 |
| **UT-GR-004** | Future Growth (50%)              | 100, 20, 10, 10, 1.5              | **15**   | Growth factor increases 10 → 15.                   |
| **UT-ZP-005** | Zero/Edge Case                   | 0, 1, 1, 1, 1.0                   | **1**    | Minimum 1 partition required.                      |

---

## II. End-to-End (E2E) Test Scenarios (Skew Validation)

Validates `checkPartitionSkew` and hot-partition detection.

### **E2E Test Table**

| Test ID        | Function           | Scenario                | Input Distribution   | Expected Result                       |
| -------------- | ------------------ | ----------------------- | -------------------- | ------------------------------------- |
| **E2E-SK-001** | checkPartitionSkew | Perfect Balance         | [100, 100, 100, 100] | **BALANCED**                          |
| **E2E-SK-002** | checkPartitionSkew | Moderate Skew (<1.5×)   | [100, 100, 150, 100] | **BALANCED** (150 = 1.33× avg)        |
| **E2E-SK-003** | checkPartitionSkew | Severe Hot Partition    | [50, 50, 200, 50]    | **SKEWED** (200 = 2.28× avg)          |
| **E2E-SK-004** | checkPartitionSkew | Multiple Hot Partitions | [100, 300, 100, 300] | **SKEWED** (300 ≥ 1.5× avg threshold) |

---
