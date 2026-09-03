# Correctness model

This library cannot make an arbitrary external HTTP/database/SaaS side effect part of a Kafka transaction. That is a distributed-systems boundary.

The strongest safe business guarantee is obtained with **idempotent external effects**:

- every external request must carry `AsyncRecordContext.recordId()` or a stable business event id as an idempotency key
- the external system must return the same result and avoid performing the side effect twice for the same key

## Modes

### FAST_IN_MEMORY_SUBMIT

Lowest latency. The input record is written to the pending store and the async operation is submitted immediately.

Use only for pure enrichment reads, idempotent operations, or test workloads.

### STORE_FIRST_DEFERRED_SUBMIT

Default. The processor writes the pending input to the Kafka Streams state store, requests a commit, and only submits the async operation from a later punctuator/recovery scan after `storeCommitBarrierDelay`.

This narrows the crash window before the external call, but the public Kafka Streams Processor API does not expose a commit-completed callback. Therefore it is not a formal two-phase transaction.

### IDEMPOTENT_EXTERNAL_EFFECT

Requires both durable stores and documents that the handler performs external side effects. The handler must use `ctx.recordId()` as idempotency key.

## What is solved

- slow work is not executed on the StreamThread
- pending work is durable
- completed outputs are durable before forwarding
- restart/rebalance resumes unfinished records
- per-key ordering can be preserved
- backpressure, retry, and metrics are explicit

## What cannot be solved inside Kafka Streams alone

This sequence cannot be made impossible for arbitrary external systems:

1. external call succeeds
2. process crashes before Kafka state/output is committed
3. record is recovered and the external call is retried

The fix is idempotency or an outbox architecture:

`Kafka Streams -> request topic -> async worker -> result topic -> Kafka Streams`
