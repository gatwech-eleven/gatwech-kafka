# Kafka Partition Strategy: The 4-Pillar Framework

This strategy outlines how to determine the optimal number of partitions and choose the correct key for a topic, based on the principles of **Throughput**, **Latency**, **Parallelism**, and **Future Growth**.

---

## Phase 1: The 4-Pillar Partition Calculation

A data-driven method to determine the **minimum number of partitions**, then adjust based on operational constraints.

---

## **Pillar 1: Throughput and Bottleneck**

Determines the absolute minimum partition count required to handle the target data volume.

### **Metrics**

| Symbol | Description                                                      |
| ------ | ---------------------------------------------------------------- |
| **T**  | Target total throughput (e.g., 200 MB/s)                         |
| **P**  | Producer throughput per partition (MB/s)                         |
| **C**  | Consumer throughput per partition (MB/s), usually the bottleneck |

### **Formula**

```
Min Partitions = max(T / P, T / C)
```

This ensures the slowest component (typically the consumer) sets the baseline.

---

## **Pillar 2: Latency**

If your latency SLO is strict (e.g., 99% of messages processed < 200ms), add more partitions than the throughput minimum. More partitions = shorter queue wait time.

---

## **Pillar 3: Consumer Parallelism**

Partitions impose a hard limit on consumer parallelism.

If the application requires **N consumers**, you must have **≥ N partitions**, even if the throughput formula suggests fewer.

---

## **Pillar 4: Future Growth (Over-Partitioning)**

Changing partition count later can break ordering guarantees.

Best practice: **over-partition by 1.5×–2×** based on projected traffic growth.

---

## Phase 2: Choosing the Partitioning Key (Avoiding Skew)

A poor key can cause **hot partitions**, destroying throughput and latency.

### **High Cardinality**

Prefer keys with many unique values and even distribution.

* **Bad (Low Cardinality):** `country_code` (e.g., 80% = "US")
* **Good (High Cardinality):** `user_id`, `transaction_id`

---

## **Ordering Requirements**

### If ordering is **NOT** required:

Use a **NULL key** → Kafka performs **round-robin**, maximizing throughput and perfect balance.

### If ordering **IS** required:

Use the **most granular ID that needs ordering** (e.g., `user_id` for per-user ordering).

---

## Advanced Skew Handling

If a single key produces massive traffic (e.g., a celebrity user's ID), use a **custom partitioner**:

* Append a random suffix (e.g., `user_123-7`) before hashing.
* Spreads load across multiple partitions.
* Sacrifices strict ordering for that specific key but keeps the system stable.

---
