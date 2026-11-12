Kafka Exactly Once Processing (E.O.P.) Implementation Plan

This document outlines the steps and configurations required to implement Kafka Transactions, ensuring Exactly Once Processing (E.O.P.) semantics for a stream processing application that follows an Input-Process-Output flow.

The core goal is to ensure that reading an input message, processing it, writing the result to one or more output topics, and committing the input offset all happen atomically, guaranteeing data consistency during failures.

Phase 1: Environment and Goal Setting

Step

Detail

1.1 Set E.O.P. Goal

The system must guarantee that every input message is processed exactly once, resulting in exactly one output message and one offset commit. This is for applications requiring exact results.

1.2 Verify Kafka Version

Ensure the Kafka cluster is running version 0.11 or later, as transaction support was introduced in this version.

1.3 Define Transaction Scope

Identify all related writes that must be atomic: (1) writing to the designated output topic(s) and (2) writing the new consumer offset to the internal __consumer_offsets topic.

Phase 2: Producer (Stream Processor) Configuration

The application acting as a stream processor (both consuming input and producing output) must be configured for transactional writes.

2.1 Producer Configuration Parameters

Parameter

Value

Purpose

enable.idempotence

true

Enables Idempotent Publishing, which prevents a producer from publishing the same batch of messages twice if the acknowledgement (ACK) is lost. This solves the duplicate producer message failure scenario.

transactional.id

A unique string (e.g., <app_name>-<instance_id>)

A mandatory ID used to register the producer with the Transaction Coordinator. This ID is essential for Fencing to prevent "Zombie Producers" (older, failed instances) from corrupting the stream.

2.2 Deep Dive: Fencing and Zombie Prevention (Epoch Numbers)

The primary defense against a new producer instance starting while an old, failed instance ("Zombie") is still running is the use of Epoch Numbers managed by the Transaction Coordinator.

Transaction Coordinator Role: The coordinator tracks the last known active instance for every unique transactional.id.

Registration and Epoch Bump: When a producer starts up, it registers its transactional.id with the coordinator. If the coordinator sees a new registration attempt for an ID that is currently active or in-flight, it assumes the old instance failed and issues a higher, incremented Epoch Number to the new instance.

Fencing: This new Epoch Number is shared with all topic leaders the new producer intends to write to. Any broker receiving a message from a producer with a lower (stale) Epoch Number for that transactional.id will reject the message.

Zombie Prevention: This mechanism effectively "fences off" the older, slower, or network-isolated "zombie" instance, guaranteeing that only the latest producer instance can successfully commit messages.

2.3 Transaction Logic Implementation (Two-Phase Commit)

The stream processor must implement a Two-Phase Commit (2PC) protocol, coordinated by the Kafka Transaction Manager, to ensure the atomicity of writes and offset commits.

Initialize Transaction: Start the transaction component by calling producer.init_transactions().

Begin Transaction: Define the start of a transactional boundary by calling producer.begin_transaction().

Process Input & Stage Output:

Read a batch of messages from the input topic.

Process the messages (apply business logic).

Send the output messages to the output topic(s) using producer.send(). These messages are written to the log but are not yet visible to downstream consumers.

Send Offsets (Critical Step): Commit the processed input offsets within the transaction using producer.send_offsets_to_transaction(). This stages the commit for the offsets alongside the output messages.

Commit Transaction: Call producer.commit_transaction(). This is the commit point. The Transaction Manager will then perform the final steps of writing commit markers to the end of all affected topic-partitions (output topics and the offsets topic).

Handle Abort: If an exception occurs during processing or writing, call producer.abort_transaction() to roll back the entire operation (messages and offsets).

Phase 3: Consumer Configuration

Downstream consumers reading the results from the output topics must be configured to only consume committed data.

Parameter

Value

Purpose

isolation.level

read_committed

MANDATORY for E.O.P. The consumer will ignore messages from a transaction until it reads the corresponding commit marker written by the Transaction Manager. This prevents reading uncommitted or aborted data.

Phase 4: Optimization and Best Practices

Best Practice

Detail

Batching

Process and commit transactions in batches rather than per single message. This is essential, as it amortizes the fixed transactional overhead (communicating with the Transaction Manager) over a larger volume of data, significantly improving overall throughput and performance.

Stateful Processing

If the processor maintains state (e.g., aggregations, counters), represent the state as a write-ahead log in a separate internal Kafka topic. The state update writes to this internal topic must also be included in the atomic transaction.

External Stores

Do not attempt to mix Kafka transactions with external stores (databases, caches) in the same atomic operation. Kafka transactions only guarantee atomicity for writes within the Kafka ecosystem (Kafka-to-Kafka).