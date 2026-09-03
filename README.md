# async-kafka-streams — Documentation

> **Requires:** Java 21 · Kafka Streams 4.x · Maven

---

## Table of Contents

1. [What Is This Library?](#1-what-is-this-library)
2. [The Problem: Why Kafka Streams Is Slow for I/O-Bound Work](#2-the-problem-why-kafka-streams-is-slow-for-io-bound-work)
3. [How This Library Solves It](#3-how-this-library-solves-it)
4. [Throughput Impact — The Numbers](#4-throughput-impact--the-numbers)
5. [Quick Start](#5-quick-start)
   - [Plain Handler API](#51-plain-handler-api)
   - [Responsive-Style Wrapper API](#52-responsive-style-wrapper-api)
   - [Spring Boot](#53-spring-boot)
6. [Core Concepts](#6-core-concepts)
   - [AsyncRecordHandler](#61-asyncrecordhandler)
   - [AsyncOutput](#62-asyncoutput)
   - [AsyncRecordContext](#63-asyncrecordcontext)
   - [Durable State Stores](#64-durable-state-stores)
   - [Ordering Modes](#65-ordering-modes)
   - [Backpressure Strategies](#66-backpressure-strategies)
   - [Error Strategies](#67-error-strategies)
   - [Correctness Modes](#68-correctness-modes)
   - [Record ID Strategy](#69-record-id-strategy)
7. [Configuration Reference](#7-configuration-reference)
8. [Processing Lifecycle (Internal Flow)](#8-processing-lifecycle-internal-flow)
9. [Correctness and Crash-Safety Model](#9-correctness-and-crash-safety-model)
   - [What the Library Guarantees](#91-what-the-library-guarantees)
   - [What It Cannot Guarantee](#92-what-it-cannot-guarantee)
   - [The Idempotency Requirement](#93-the-idempotency-requirement)
   - [Strongest Configuration](#94-strongest-configuration)
10. [Use Cases — When to Use This Library](#10-use-cases--when-to-use-this-library)
11. [Limitations — When NOT to Use This Library](#11-limitations--when-not-to-use-this-library)
12. [The Strict Outbox Pattern (For Non-Idempotent Work)](#12-the-strict-outbox-pattern-for-non-idempotent-work)
13. [Metrics and Observability](#13-metrics-and-observability)
    - [AsyncMetricsListener](#131-asyncmetricslistener)
    - [Micrometer Integration](#132-micrometer-integration)
    - [Metric Reference](#133-metric-reference)
14. [Spring Boot Integration](#14-spring-boot-integration)
15. [Operations Guide](#15-operations-guide)
    - [Alerting](#151-alerting)
    - [Tuning Rules](#152-tuning-rules)
    - [Scaling](#153-scaling)
16. [Developer Internals](#16-developer-internals)
    - [Module Map](#161-module-map)
    - [Internal State Machines](#162-internal-state-machines)
    - [Thread Safety Model](#163-thread-safety-model)
    - [The `drainReady` / `drainCompleted` Loop](#164-the-drainready--draincompleted-loop)
    - [Key-Ordering Algorithm](#165-key-ordering-algorithm)
    - [Buffer Counting](#166-buffer-counting)
    - [Recovery on Restart](#167-recovery-on-restart)
    - [The Responsive-Style Wrapper (AsyncFixedKeyProcessor)](#168-the-responsive-style-wrapper-asyncfixedkeyprocessor)
17. [Build, Test, and CI](#17-build-test-and-ci)
18. [FAQ](#18-faq)

---

## 1. What Is This Library?

`async-kafka-streams` is a dependency extension for the **Kafka Streams Processor API** that lets you run I/O-bound work (HTTP calls, database queries, gRPC calls, …) **in parallel** without blocking the Kafka Streams stream thread, while keeping the same at-least-once durability and per-key ordering guarantees that Kafka Streams normally provides.

It is a self-contained open implementation of the same architectural idea that commercial products like Responsive SDK offer, built entirely on public Kafka Streams API.

The library ships two integration styles:

| Style | API entry point | Best for |
|---|---|---|
| **Handler API** | `AsyncProcessorSupplier.create(handler, options)` | Greenfield; the async work is already a `CompletionStage` (WebClient, async HTTP client, etc.) |
| **Wrapper API** | `AsyncProcessorSuppliers.wrapValues(delegate, options)` | Existing processors with blocking I/O; drop-in acceleration |

---

## 2. The Problem: Why Kafka Streams Is Slow for I/O-Bound Work

Kafka Streams processes one record at a time **on the stream thread**. If each record requires an external HTTP call that takes 20 ms, the topology can only handle 50 records/second per partition — regardless of how many CPU cores or network connections you have.

### Without this library

```
Kafka Streams stream thread (single-threaded per task)
│
├─ receive record-1
│   └─ call external HTTP  ──── 20 ms wait ────→ forward result
├─ receive record-2
│   └─ call external HTTP  ──── 20 ms wait ────→ forward result
├─ receive record-3
│   └─ call external HTTP  ──── 20 ms wait ────→ forward result
...
Throughput: 50 records/s (one at a time, fully sequential)
```

All the time is wasted **waiting** for the external system. Adding more partitions helps, but every instance gets its own stream thread and topic partition — you scale at enormous infrastructure cost.

**Typical workarounds and their problems:**

| Workaround | Problem |
|---|---|
| More partitions | Repartitions change key routing, break ordering, add latency |
| More stream threads | Each thread owns a partition — more threads means more partitions anyway |
| Pre-cache everything in RocksDB | Only works for reads; external mutations still need a call |
| Custom `Thread` inside a processor | State store access from non-stream threads is undefined behavior in Kafka Streams |
| Build a second microservice | Adds two extra Kafka topics, extra deployment, extra latency |

### With this library

```
Kafka Streams stream thread
│
├─ receive record-1  →  enqueue to async executor
├─ receive record-2  →  enqueue to async executor
├─ receive record-3  →  enqueue to async executor
...
├─ punctuate: drain completed results from async worker pool
│    ├─ record-1 result ready  →  persist output  →  forward
│    ├─ record-2 result ready  →  persist output  →  forward
│    └─ record-3 result ready  →  persist output  →  forward

Async executor pool runs in parallel:
  thread-1: call HTTP for record-1  ──── 20 ms ────→ done
  thread-2: call HTTP for record-2  ──── 20 ms ────→ done      } all at once
  thread-3: call HTTP for record-3  ──── 20 ms ────→ done
```

**Throughput: 50 × `maxInFlight` records/s on the same single partition.**

With `maxInFlight=50` and 20 ms I/O: **2500 records/s** — a **50× speedup** on the same partition without any topology change.

---

## 3. How This Library Solves It

The library inserts an asynchronous boundary inside a standard Kafka Streams processor node. The stream thread only:
1. Receives the record and enqueues it.
2. Periodically drains the completion queue (punctuate).
3. Forwards results to downstream topology nodes.

All actual I/O work is done by an **Executor** that you supply (virtual threads, thread pool, etc.), completely outside the Kafka Streams scheduling.

To ensure **crash safety** (so that work is not lost if the JVM dies while an async call is in flight), the library optionally persists:
- The **input record** (pending store) — so recovery after restart can re-submit unfinished work.
- The **output records** (output store) — so completed results are not re-emitted if the JVM dies between computation and forwarding.

Both stores are regular Kafka Streams persistent `KeyValueStore` (RocksDB), backed by changelog topics, giving them the same durability as any other Kafka Streams state store.

---

## 4. Throughput Impact — The Numbers

The integration test `ThroughputIT` runs both a synchronous and an async topology against a real Kafka broker (via Testcontainers) with 200 records and 20 ms simulated I/O each:

```
=== THROUGHPUT BENCHMARK (200 records, 20 ms I/O each) ===
  Synchronous           : 5217 ms  (38 records/s)
  Async (50 in-flight)  : 1027 ms  (194 records/s)
  Speedup               : 5.1×
```

The async topology completes in **~1 second** because up to 50 records are in flight simultaneously. The synchronous topology serializes all 200 × 20 ms = 4000 ms of pure I/O wait.

The speedup equals `min(maxInFlight, recordCount) × ioLatency / totalAsyncTime` — capped at `maxInFlight` because that many records run in parallel. With real async HTTP clients (WebClient, Vert.x, etc.) and virtual threads, `maxInFlight` values of 500–2000 are normal.

---

## 5. Quick Start

### 5.1 Plain Handler API

Use this when your work is naturally expressed as a `CompletionStage` — for example, using Spring WebClient or Java's `CompletableFuture`.

**Step 1: Add the dependency**

```xml
<dependency>
    <groupId>kafka.streams.async</groupId>
    <artifactId>async-kafka-streams</artifactId>
    <version>0.6.0-SNAPSHOT</version>
</dependency>
```

**Step 2: Write your handler**

```java
AsyncRecordHandler<String, InputEvent, String, OutputEvent> handler =
    (record, ctx) -> webClient.post()
        .uri("/enrich")
        .header("Idempotency-Key", ctx.recordId())  // ALWAYS send this
        .bodyValue(record.value())
        .retrieve()
        .bodyToMono(OutputEvent.class)
        .map(out -> List.of(AsyncOutput.of(record.key(), out)))
        .toFuture();
```

**Step 3: Configure options**

```java
String pendingStore = "enrich-pending";
String outputStore  = "enrich-output";
Executor executor   = Executors.newVirtualThreadPerTaskExecutor();

AsyncProcessorOptions<String, InputEvent, String, OutputEvent> options =
    AsyncProcessorOptions.<String, InputEvent, String, OutputEvent>builder(executor)
        .processorName("customer-enrichment")
        .pendingStoreName(pendingStore)
        .outputStoreName(outputStore)
        .ordering(Ordering.KEY)
        .maxInFlight(500)
        .maxBufferedRecords(10_000)
        .maxAttempts(5)
        .initialBackoff(Duration.ofSeconds(1))
        .maxBackoff(Duration.ofMinutes(5))
        .backpressureStrategy(BackpressureStrategy.FAIL)
        .errorStrategy(ErrorStrategy.FAIL_TASK)
        .build();
```

**Step 4: Wire into the topology**

```java
Topology topology = new Topology();
topology.addSource("source",
    Serdes.String().deserializer(),
    inputEventSerde.deserializer(),
    "input-topic");

topology.addProcessor("async-enrich",
    AsyncProcessorSupplier.create(handler, options),
    "source");

// Attach the two durable state stores to the processor
topology.addStateStore(
    AsyncStores.pendingStore(pendingStore, Serdes.String(), inputEventSerde),
    "async-enrich");
topology.addStateStore(
    AsyncStores.outputStore(outputStore, Serdes.String(), outputEventSerde),
    "async-enrich");

topology.addSink("sink",
    "output-topic",
    Serdes.String().serializer(),
    outputEventSerde.serializer(),
    "async-enrich");
```

Or with the Streams DSL (`StreamsBuilder`):

```java
StreamsBuilder builder = new StreamsBuilder();
builder.addStateStore(AsyncStores.pendingStore(pendingStore, Serdes.String(), inputEventSerde));
builder.addStateStore(AsyncStores.outputStore(outputStore, Serdes.String(), outputEventSerde));

builder.stream("input-topic", Consumed.with(Serdes.String(), inputEventSerde))
    .process(
        AsyncProcessorSupplier.create(handler, options),
        Named.as("async-enrich"),
        pendingStore, outputStore)
    .to("output-topic", Produced.with(Serdes.String(), outputEventSerde));
```

---

### 5.2 Responsive-Style Wrapper API

Use this when you have an **existing** blocking `FixedKeyProcessor` and want to accelerate it without rewriting it.

```java
// Your existing processor (unchanged)
public class CustomerEnrichmentProcessor
        implements FixedKeyProcessor<String, Order, EnrichedOrder> {

    private final CustomerClient client;
    private FixedKeyProcessorContext<String, EnrichedOrder> context;

    @Override public void init(FixedKeyProcessorContext<String, EnrichedOrder> ctx) {
        this.context = ctx;
    }

    @Override public void process(FixedKeyRecord<String, Order> record) {
        Customer c = client.fetchCustomer(record.value().customerId()); // blocking I/O
        context.forward(record.withValue(new EnrichedOrder(record.value(), c)));
    }
}

// Wrap it for async execution — just change the supplier in processValues:
builder.stream("orders", Consumed.with(Serdes.String(), orderSerde))
    .processValues(
        AsyncProcessorSuppliers.wrapValues(
            () -> new CustomerEnrichmentProcessor(customerClient),
            asyncOptions
        ),
        Named.as("async-customer-enrichment"),
        PENDING_STORE,     // async pending store
        OUTPUT_STORE       // async output store
    )
    .to("orders-enriched", Produced.with(Serdes.String(), enrichedOrderSerde));
```

The wrapped processor's `context.forward(...)` calls are **captured** inside a buffered context, persisted to the output store, and then replayed from the Kafka Streams thread at punctuation time — not from the worker thread.

> **Important:** The wrapped processor must **not** access Kafka Streams state stores from the worker thread (see [State Store Access Policy](#68-correctness-modes)). The default policy is `DISALLOW` and will throw if violated.

---

### 5.3 Spring Boot

Add the dependency. The autoconfiguration registers:
- An `asyncKafkaStreamsExecutor` bean (fixed thread pool, 32 threads by default).
- An `AsyncMetricsListener` bean (noop, or Micrometer-backed if `MeterRegistry` is detected).

```yaml
kafka:
  streams:
    async:
      executor-threads: 64
      processor-name: customer-enrichment
      max-in-flight: 500
      max-buffered-records: 10000
      punctuate-interval: 100ms
      recovery-scan-interval: 5s
      store-commit-barrier-delay: 1s
      correctness-mode: STORE_FIRST_DEFERRED_SUBMIT
```

Inject the executor and listener into your topology bean:

```java
@Configuration
public class AsyncTopologyConfig {
    @Bean
    Topology topology(
            @Qualifier("asyncKafkaStreamsExecutor") Executor executor,
            AsyncMetricsListener metricsListener,
            WebClient webClient) {

        var options = AsyncProcessorOptions.<String, Event, String, Result>builder(executor)
            .metricsListener(metricsListener)
            // ... other options ...
            .build();
        // ...
    }
}
```

---

## 6. Core Concepts

### 6.1 AsyncRecordHandler

```java
@FunctionalInterface
public interface AsyncRecordHandler<KIn, VIn, KOut, VOut> {
    CompletionStage<Collection<AsyncOutput<KOut, VOut>>> process(
            Record<KIn, VIn> record,
            AsyncRecordContext context
    );
}
```

Your handler receives the input `Record` and an `AsyncRecordContext` and must return a `CompletionStage`. The stage completes with a collection of output records to forward, or completes exceptionally if the work fails (triggering retry).

- Returning `null` or an empty collection is valid — no output will be forwarded.
- **The handler runs on the async executor thread**, never on the Kafka Streams stream thread. Do not interact with Kafka Streams API from inside the handler.
- The handler is called once per attempt. `context.attempt()` starts at `1`.

### 6.2 AsyncOutput

```java
public record AsyncOutput<K, V>(K key, V value, Long timestamp, String childName) {
    public static <K, V> AsyncOutput<K, V> of(K key, V value) { ... }
    public static <K, V> AsyncOutput<K, V> of(K key, V value, long timestamp) { ... }
    public static <K, V> AsyncOutput<K, V> toChild(K key, V value, String childName) { ... }
}
```

Each `AsyncOutput` represents one downstream record to forward. A single input record can produce zero, one, or many outputs.

- `timestamp` — if `null`, the original input record's timestamp is used.
- `childName` — if set, the record is forwarded to the named child node; otherwise it is forwarded to all downstream nodes.

### 6.3 AsyncRecordContext

```java
public record AsyncRecordContext(
        String recordId,          // stable ID used as idempotency key
        int attempt,              // 1-based attempt number
        Optional<RecordMetadata> recordMetadata  // source topic/partition/offset
) {}
```

`recordId` is the most important field. Always pass it to external systems as an idempotency key. By default it is `topic:partition:offset` — stable across retries for Kafka input records.

### 6.4 Durable State Stores

The library uses two persistent RocksDB-backed `KeyValueStore`s:

| Store | Purpose | When used |
|---|---|---|
| **Pending store** | Stores the input record before async work starts | Required for crash recovery and retries |
| **Output store** | Stores completed results before they are forwarded | Required to avoid re-executing the handler after a crash during forwarding |

Both are optional: if you omit `pendingStoreName` and `outputStoreName`, the library runs in **in-memory-only** mode — faster, but no crash recovery.

Create them with `AsyncStores`:

```java
AsyncStores.pendingStore(storeName, keySerde, valueSerde);
AsyncStores.outputStore(storeName, keySerde, valueSerde);
```

The pending store holds `AsyncPendingRecord` entries that include the full input record, the current attempt number, the `notBeforeEpochMs` (for backoff), and the last error message.

The output store holds `AsyncOutputRecord` entries that include serialized output key/value, timestamp, headers, child processor name, sequence number, and a terminal marker.

### 6.5 Ordering Modes

```java
.ordering(Ordering.KEY)       // default
.ordering(Ordering.UNORDERED)
```

**`KEY` (default):** Only one record with a given Kafka key is in-flight at a time. All subsequent records with the same key queue up in `waitingByKey` and are released one by one only after the current record completes (including retries). This preserves the ordering that Kafka Streams normally guarantees within a partition.

**`UNORDERED`:** Maximum throughput. All records are submitted to the executor as fast as permits allow. Outputs may arrive and be forwarded in a different order from inputs. Use this only when order does not matter (e.g. stateless enrichment that writes to a compacted topic with the same key).

> **Rule of thumb:** If you are calling a REST API to enrich a stateless lookup, `UNORDERED` is fine. If you are writing state (database upserts, domain events), use `KEY` so you do not race against yourself.

### 6.6 Backpressure Strategies

When `bufferedRecords() >= maxBufferedRecords`, the library applies the configured strategy:

| Strategy | Behavior | When to use |
|---|---|---|
| `FAIL` (default) | Throws `StreamsException` on the stream thread, which causes the Kafka Streams task to fail. Kafka Streams then restarts the task per your `RestoreListener` / `StreamsUncaughtExceptionHandler`. | Safest default. Lets Kafka Streams backpressure itself. |
| `BLOCK` | Blocks the stream thread for up to `backpressureBlockTimeout` (default 2 s), draining work in a loop. If the buffer does not clear in time, falls through to `FAIL`. | Use when brief stalls are acceptable and you want to avoid task restarts. Be careful: blocking the stream thread for too long can trigger consumer group rebalance. |
| `DROP` | Calls `AsyncErrorHandler.onFailure(...)` and skips the record permanently (one call to the error handler, zero retries). | Lossy pipelines only (e.g. telemetry, dashboards). Never for financial data. |

### 6.7 Error Strategies

After `maxAttempts` retries are exhausted:

| Strategy | Behavior |
|---|---|
| `FAIL_TASK` (default) | Throws `StreamsException` on the stream thread. The task fails. Kafka Streams restarts it per your uncaught exception handler. The pending record survives in the pending store and will be re-attempted after restart. |
| `SKIP` | Calls `AsyncErrorHandler.onFailure(...)` and permanently skips the record. Logs a warning. No exception is thrown; the topology continues. |

### 6.8 Correctness Modes

Controls when the async work is submitted relative to the Kafka Streams state store commit:

| Mode | Description | Use when |
|---|---|---|
| `FAST_IN_MEMORY_SUBMIT` | Submits async work immediately after writing the input to the pending store, without waiting for a commit. | Pure enrichment reads (no side effects), idempotent operations, performance-critical paths. |
| `STORE_FIRST_DEFERRED_SUBMIT` (default) | Writes the pending record, calls `context.commit()`, then submits from the recovery scan after `storeCommitBarrierDelay` (default 1 s). Reduces the crash window before the external call. | All production workloads with external side effects. This is the safe default. |
| `IDEMPOTENT_EXTERNAL_EFFECT` | Same as `STORE_FIRST_DEFERRED_SUBMIT` but explicitly documents that the handler uses `ctx.recordId()` as an idempotency key for mutating external calls. Requires both pending and output stores. | Mutating APIs (REST PUT/POST) that support idempotency keys, e.g. payment providers, SaaS APIs. |

**None of these modes make an external HTTP call part of a Kafka transaction.** They reduce the window during which a crash could cause a duplicate external call. Only idempotency in the external system truly eliminates duplicates.

**State Store Access Policy** (for the wrapper API):

```java
.stateStoreAccessPolicy(StateStoreAccessPolicy.DISALLOW)    // default
.stateStoreAccessPolicy(StateStoreAccessPolicy.ALLOW_UNSAFE)
```

- `DISALLOW`: If the wrapped processor calls `context.getStateStore(...)` from the async worker thread, an `IllegalStateException` is thrown. This is the safe default.
- `ALLOW_UNSAFE`: Allows state store access from the worker thread by delegating to the real `ProcessorContext`. Only use this if you have carefully analyzed thread safety for your specific store and processor.

### 6.9 Record ID Strategy

```java
.recordIdStrategy(RecordIdStrategy.topicPartitionOffset())  // default
```

The default strategy produces IDs in the form `topic:partition:offset`, which is stable across retries for records consumed from a Kafka topic.

For records created synthetically (e.g. from a `KStream.flatMap`) there may be no source metadata, and the default falls back to a random UUID — which means each retry gets a different ID, breaking idempotency.

For business records, prefer a stable business identifier:

```java
.recordIdStrategy((record, metadata) -> record.value().getBusinessEventId())
```

---

## 7. Configuration Reference

All options are set via `AsyncProcessorOptions.Builder`:

| Method | Default | Description |
|---|---|---|
| `executor(Executor)` | **required** | The executor that runs async handler calls. Use virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) for I/O-bound work or a fixed thread pool for CPU-bound work. |
| `processorName(String)` | `"async-processor"` | Label used in logs and metrics. |
| `pendingStoreName(String)` | `null` (disabled) | RocksDB store name for durable pending records. Required for crash recovery. |
| `outputStoreName(String)` | `null` (disabled) | RocksDB store name for durable output records. Required for crash recovery. Requires `pendingStoreName` also set. |
| `ordering(Ordering)` | `KEY` | Per-key ordering (`KEY`) or maximum parallelism (`UNORDERED`). |
| `maxInFlight(int)` | `100` | Maximum number of records submitted to the executor but not yet completed. Controls parallelism. |
| `maxBufferedRecords(int)` | `10,000` | Maximum total records tracked (in-flight + waiting). Must be `>= maxInFlight`. Exceeding this triggers the backpressure strategy. |
| `maxAttempts(int)` | `3` | Total attempts per record (first attempt + retries). |
| `initialBackoff(Duration)` | `1 s` | Backoff for attempt 1 → 2. Doubles with each subsequent attempt (exponential backoff). |
| `maxBackoff(Duration)` | `60 s` | Maximum backoff cap. |
| `backpressureStrategy(BackpressureStrategy)` | `FAIL` | What to do when the buffer is full (see [§6.6](#66-backpressure-strategies)). |
| `errorStrategy(ErrorStrategy)` | `FAIL_TASK` | What to do when retries are exhausted (see [§6.7](#67-error-strategies)). |
| `correctnessMode(CorrectnessMode)` | `STORE_FIRST_DEFERRED_SUBMIT` | When to submit async work relative to store commit (see [§6.8](#68-correctness-modes)). |
| `storeCommitBarrierDelay(Duration)` | `1 s` | How long after the pending store write to wait before submitting the async work (for `STORE_FIRST_DEFERRED_SUBMIT` and `IDEMPOTENT_EXTERNAL_EFFECT`). |
| `punctuateInterval(Duration)` | `100 ms` | How often the stream thread checks for completed async work and forwards outputs. |
| `recoveryScanInterval(Duration)` | `5 s` | How often the stream thread scans the pending store to re-submit unfinished records after restart. |
| `outputDrainBatchSize(int)` | `1,000` | Maximum number of output records forwarded per drain cycle. Prevents a very large completion from monopolizing the punctuate time slot. |
| `recoveryScanMaxRecords(int)` | `10,000` | Maximum pending store entries scanned per recovery interval. |
| `backpressureBlockTimeout(Duration)` | `2 s` | How long `BLOCK` strategy can hold the stream thread before failing. |
| `recordIdStrategy(RecordIdStrategy)` | `topicPartitionOffset()` | How to compute the stable record ID used as a key in the pending store. |
| `errorHandler(AsyncErrorHandler)` | `noop` | Callback invoked when a record is skipped or when a DROP backpressure fires. |
| `metricsListener(AsyncMetricsListener)` | `noop` | Callback for all metric events. Use `MicrometerAsyncMetricsListener` for Prometheus/Grafana. |
| `stateStoreAccessPolicy(StateStoreAccessPolicy)` | `DISALLOW` | Whether the wrapped (Responsive-style) processor may access state stores from the worker thread. |

---

## 8. Processing Lifecycle (Internal Flow)

This shows the complete path of one record through the library:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      KAFKA STREAMS STREAM THREAD                     │
│                                                                       │
│  1. process(record)                                                   │
│     │                                                                 │
│     ├─ Compute record ID  (RecordIdStrategy)                         │
│     ├─ Check capacity     (bufferedRecords < maxBufferedRecords)      │
│     ├─ Write to pendingStore  ← RocksDB changelog topic              │
│     ├─ enqueueIfNotInFlight → ready queue  (or waitingByKey)         │
│     └─ drainReady()                                                   │
│                                                                       │
│  2. drainReady()  (called from process() and punctuate())            │
│     │                                                                 │
│     ├─ For each item in ready queue (up to maxInFlight permits):      │
│     │   ├─ Check notBeforeEpochMs  (backoff not expired → re-queue)  │
│     │   ├─ Check inFlightIds       (already running → skip)          │
│     │   ├─ Check hasStoredOutput   (already done → skip)             │
│     │   ├─ KEY ordering: check activeKeys (conflict → waitingByKey)  │
│     │   └─ submit(item)  → executor.execute(lambda)                  │
│     │                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │                    ASYNC EXECUTOR POOL                       │     │
│  │                                                               │     │
│  │  lambda runs:                                                 │     │
│  │    handler.process(record, ctx)                               │     │
│  │    .whenComplete((outputs, error) →                          │     │
│  │        completed.offer(new Completed(item, outputs, error))) │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                       │
│  3. onPunctuate()  (every punctuateInterval, e.g. 100 ms)            │
│     ├─ drainStoredOutputs()  (forward previously stored outputs)     │
│     ├─ drainCompleted()                                              │
│     │   ├─ success: handleSuccess() → write to outputStore           │
│     │   └─ failure: handleFailure() → retry or exhaust               │
│     ├─ drainStoredOutputs()  (forward newly stored outputs)          │
│     └─ drainReady()          (submit next wave)                      │
│                                                                       │
│  4. handleSuccess(item, outputs)                                      │
│     ├─ Serialize each AsyncOutput → AsyncOutputRecord                │
│     ├─ Write to outputStore  ← RocksDB (crash-safe)                  │
│     └─ (forwarding deferred to drainStoredOutputs)                   │
│                                                                       │
│  5. drainStoredOutputs()                                              │
│     ├─ Load outputStore batch (sorted by recordId:sequence)         │
│     ├─ context.forward(record) for each output                       │
│     ├─ Delete from outputStore                                        │
│     └─ When no more outputs for a recordId:                         │
│         ├─ pendingStore.delete(recordId)                            │
│         └─ finishRecord → release key lane (KEY ordering)         │
│                                                                       │
│  6. handleFailure(item, error)                                        │
│     ├─ attempt < maxAttempts:                                         │
│     │   ├─ Compute nextAttempt + exponential backoff notBefore        │
│     │   ├─ Write updated AsyncPendingRecord to pendingStore          │
│     │   └─ ready.offer(retry)  (will sleep until notBefore)          │
│     └─ attempt >= maxAttempts:                                        │
│         ├─ errorHandler.onFailure(record, error, attempt)            │
│         ├─ pendingStore.delete(recordId)                            │
│         └─ FAIL_TASK: throw StreamsException                         │
│            SKIP: log warning, continue                                │
│                                                                       │
│  7. scanPersistentStores()  (every recoveryScanInterval, e.g. 5 s)   │
│     ├─ drainStoredOutputs()  (forward completed-but-not-forwarded)   │
│     ├─ scanPendingStore():                                            │
│     │   └─ For each entry in pendingStore where notBefore <= now:    │
│     │       └─ enqueueIfNotInFlight(item) → ready queue              │
│     └─ drain loop (for in-memory/test executors)                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 9. Correctness and Crash-Safety Model

### 9.1 What the Library Guarantees

With durable stores enabled:

| Scenario | Recovery |
|---|---|
| JVM crashes after input is received but before async work starts | Pending store restores the input; next scan re-submits it |
| JVM crashes while async work is in flight (call not sent yet) | Pending store retries the input |
| JVM crashes while async work is in flight (call already sent, result not received) | Pending store retries the input; **external system may receive a duplicate call** |
| JVM crashes after completion is stored but before forwarding | Output store forwards it after restart |
| Rebalance while work is in flight | Task is reassigned; new task recovers from stores |

### 9.2 What It Cannot Guarantee

**This is the fundamental limit:**

```
1. Handler calls external system
2. External system succeeds and returns
3. JVM dies before the stream thread persists the output
4. Record is recovered from the pending store and re-submitted
5. External system receives the call a second time
```

This sequence **cannot be eliminated** inside Kafka Streams alone because the Kafka Streams `ProcessorContext` API does not expose a committed-callback that would let you atomically persist the result and acknowledge the completion.

Even with `processing.guarantee=exactly_once_v2`, exactly-once applies to Kafka state stores and output topics only — not to arbitrary external systems.

### 9.3 The Idempotency Requirement

To handle the duplicate-call scenario safely, **every external system call must carry a stable idempotency key**:

```java
handler = (record, ctx) ->
    webClient.post()
        .uri("/api/process")
        .header("Idempotency-Key", ctx.recordId())  // <-- always do this
        .bodyValue(record.value())
        .retrieve()
        .bodyToMono(Output.class)
        .map(out -> List.of(AsyncOutput.of(record.key(), out)))
        .toFuture();
```

`ctx.recordId()` defaults to `topic:partition:offset`. If your business domain has a stable event/command ID, prefer that:

```java
.recordIdStrategy((record, metadata) -> record.value().getEventId())
```

> **Non-negotiable rule:** If your external system does not support idempotency keys (and you cannot add them), this library cannot safely process mutating side effects. Use the outbox pattern instead (see [§12](#12-the-strict-outbox-pattern-for-non-idempotent-work)).

### 9.4 Strongest Configuration

For the highest durability, combine:

```properties
# Kafka Streams config
processing.guarantee=exactly_once_v2
commit.interval.ms=100
```

```java
// Processor options
.correctnessMode(CorrectnessMode.IDEMPOTENT_EXTERNAL_EFFECT)
.storeCommitBarrierDelay(Duration.ofSeconds(2))
.maxAttempts(5)
.initialBackoff(Duration.ofSeconds(1))
.maxBackoff(Duration.ofMinutes(5))
.errorStrategy(ErrorStrategy.FAIL_TASK)
```

This means:
- Kafka exactly-once for all state/output topic writes.
- Pending record is committed to RocksDB before any external call is attempted.
- A 2-second barrier gives Kafka Streams time to commit the pending record before the async call goes out.
- Up to 5 attempts with exponential backoff: 1 s, 2 s, 4 s, 8 s, 16 s.
- Any persistent failure crashes the task (instead of silently skipping).

---

## 10. Use Cases — When to Use This Library

✅ **Good fits:**

| Use case | Why it fits |
|---|---|
| **HTTP enrichment** — call a REST API to add data to every record (customer profiles, product catalogs, geo lookups) | Classic read-only, idempotent call. Very common in real-world streaming pipelines. `UNORDERED` or `KEY` ordering depending on whether downstream cares about order. |
| **Bulk database reads** — look up related data from a relational DB or NoSQL store | A `SELECT` is idempotent. The library turns sequential lookups into parallel batches. |
| **Idempotent REST mutations** — PATCH/PUT with idempotency keys (Stripe, Twilio, etc.) | Pass `ctx.recordId()` as the idempotency key. The external system deduplicates retries. |
| **gRPC / async RPC calls** | The `CompletionStage` API composes directly with gRPC's `ListenableFuture`/reactive stubs. |
| **Fan-out enrichment** — one input produces multiple outputs | Return multiple `AsyncOutput` instances from your handler. |
| **Legacy blocking processor acceleration** (wrapper API) | Drop-in: wrap your existing `FixedKeyProcessor` with `AsyncProcessorSuppliers.wrapValues(...)`. |
| **Rate-limited external APIs** | Set `maxInFlight` to match the rate limit (e.g. 100 req/s → `maxInFlight=100` with 1 s I/O delay). |
| **High-volume enrichment pipelines** that cannot afford more Kafka partitions | Multiply throughput within a single partition without infrastructure changes. |

✅ **Ordering use cases:**

- `Ordering.KEY`: Any pipeline where you need customer/account/entity state to evolve sequentially — e.g. account balance enrichment, order state machine updates, per-user rate limiting.
- `Ordering.UNORDERED`: Stateless lookups, telemetry enrichment, read-only joins where the response is independent of previous records.

---

## 11. Limitations — When NOT to Use This Library

❌ **Not suitable / requires careful consideration:**

| Situation | Reason | Alternative |
|---|---|---|
| **Non-idempotent mutations without idempotency key support** (send email, charge payment, fire SMS) | Crash-then-retry will execute the side effect twice. | Use the **strict outbox pattern** (see [§12](#12-the-strict-outbox-pattern-for-non-idempotent-work)). |
| **Processor needs Kafka Streams state stores from the worker thread** | State stores are owned by the stream task thread. Cross-thread access is a data race. | Snapshot the state you need before the async boundary (read it on the stream thread, pass into the handler). |
| **CPU-bound work** (compression, serialization, aggregation) | The stream thread is not the bottleneck here. Adding parallelism inside a partition does not help if the CPU is already saturated. | Scale partitions and stream threads. |
| **Extremely low latency requirements** (< 5 ms end-to-end per record) | The punctuate interval (default 100 ms) adds latency. Results are forwarded at the next punctuation, not instantly. | Reduce `punctuateInterval` to 10–20 ms, but be aware this increases stream thread overhead. |
| **Large in-flight state with short rebalance deadlines** | During rebalance, all in-flight work is lost. Recovery scans the pending store but this takes time. If your consumer group has a tight `max.poll.interval.ms`, the task may fail while recovering. | Tune `max.poll.interval.ms` or reduce `maxInFlight`. |
| **Transactions spanning the async boundary** (read-modify-write in a RDBMS) | The library cannot enroll an external database transaction in the Kafka transaction. The window for a partial commit always exists. | Use a SAGA pattern or the outbox pattern. |
| **Exactly-once to external systems** | Not possible through this library alone. Exactly-once for Kafka state/topics only. | Transactional outbox with exactly-once producers if you own the downstream system. |
| **High record fan-out combined with large output store** | Each input can produce many `AsyncOutput` records, all written to RocksDB. With millions of records and large outputs, RocksDB storage can grow rapidly. | Set `outputDrainBatchSize` generously and monitor `output.store.entries`. |
| **Multiple stream threads on the same JVM** | Each stream thread has its own processor instance and its own durable stores. Ordering is per-thread per-partition — no global ordering. | This is correct by Kafka's partition semantics, but may be surprising if you expect cross-partition ordering. |

---

## 12. The Strict Outbox Pattern (For Non-Idempotent Work)

If your use case **requires exactly-once execution** of a side effect (payments, sending emails, account mutations in a RDBMS without idempotency key support), do **not** run the side effect inside the Kafka Streams processor.

Use this architecture instead:

```
┌─────────────────────────────────────────────────────┐
│           Kafka Streams Topology (Tier 1)            │
│                                                       │
│  Input Topic ──→ [transform only] ──→ Request Topic  │
│                                                       │
│  (No external side effects here. Pure Kafka.)         │
└─────────────────────────────────────────────────────┘
                          │
                          ▼ Kafka: at-least-once delivery
┌─────────────────────────────────────────────────────┐
│       External Worker Service (Tier 2)               │
│                                                       │
│  Request Topic ──→ [execute side effect]             │
│     with idempotency check + own retry logic         │
│     ──→ Result Topic                                 │
│                                                       │
│  (This service owns the idempotency and retries.)    │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│           Kafka Streams Topology (Tier 3)            │
│                                                       │
│  Result Topic ──→ [continue processing]              │
└─────────────────────────────────────────────────────┘
```

This is the only pattern that avoids pretending an arbitrary HTTP service participates in a Kafka transaction. The async processor in this library is still useful for **Tier 1 → Request Topic** enrichment, but the actual mutation lives in Tier 2.

---

## 13. Metrics and Observability

### 13.1 AsyncMetricsListener

Implement `AsyncMetricsListener` to receive events for every processing milestone:

```java
public interface AsyncMetricsListener {
    default void onRecordReceived(String processorName, String recordId) { }
    default void onRecordSubmitted(String processorName, String recordId, int attempt) { }
    default void onRecordSucceeded(String processorName, String recordId) { }
    default void onRecordFailed(String processorName, String recordId, int attempt, Throwable error) { }
    default void onRecordRetried(String processorName, String recordId, int nextAttempt, long notBeforeEpochMs) { }
    default void onRecordSkipped(String processorName, String recordId) { }
    default void onRecordForwarded(String processorName, String recordId, int sequence) { }
    default void onBackpressure(String processorName, String recordId, BackpressureStrategy strategy) { }
    default void onSnapshot(String processorName, AsyncMetricsSnapshot snapshot) { }
}
```

`onSnapshot` is called at every `punctuateInterval` with a full gauge snapshot.

### 13.2 Micrometer Integration

If `micrometer-core` is on the classpath:

```java
MicrometerAsyncMetricsListener listener =
    new MicrometerAsyncMetricsListener(meterRegistry, "customer-enrichment");

options = AsyncProcessorOptions.builder(executor)
    .metricsListener(listener)
    .build();
```

In Spring Boot this is wired automatically when a `MeterRegistry` bean is present.

### 13.3 Metric Reference

**Counters** (monotonically increasing):

| Metric name | Description |
|---|---|
| `kafka.streams.async.records.received` | Total input records received by the processor |
| `kafka.streams.async.records.submitted` | Total records submitted to the executor (includes retry attempts) |
| `kafka.streams.async.records.succeeded` | Total records that completed successfully |
| `kafka.streams.async.records.failed` | Total failed attempts (including those that will be retried) |
| `kafka.streams.async.records.retried` | Total retry attempts scheduled |
| `kafka.streams.async.records.skipped` | Records permanently dropped (SKIP strategy or DROP backpressure) |
| `kafka.streams.async.records.forwarded` | Total output records forwarded downstream |
| `kafka.streams.async.backpressure.events` | Times the buffer was full |

**Gauges** (current value):

| Metric name | Description |
|---|---|
| `kafka.streams.async.inflight` | Records currently executing on the async executor |
| `kafka.streams.async.scheduled` | All records tracked by the processor (in-flight + ready + waiting) |
| `kafka.streams.async.ready.queue` | Records queued and ready to submit (not yet acquired a permit) |
| `kafka.streams.async.completed.queue` | Completion callbacks not yet drained by the stream thread |
| `kafka.streams.async.pending.store.entries` | Approximate RocksDB pending store entry count |
| `kafka.streams.async.output.store.entries` | Approximate RocksDB output store entry count |

All metrics carry a `processor` tag with the value of `processorName`.

**Recommended alerts:**

```
pending.store.entries continuously growing → processor is stuck; async calls are failing or not draining
output.store.entries continuously growing → forwarding is stuck; stream thread not running punctuate
inflight pinned at maxInFlight → executor is saturated or external system is very slow
backpressure.events > 0 → increase maxBufferedRecords or maxInFlight or reduce input rate
records.retried increasing fast → external system is degraded
records.skipped > 0 → records are being permanently lost (check logs)
records.failed and records.retried diverging → retry loop is not succeeding
```

---

## 14. Spring Boot Integration

The library ships a Spring Boot autoconfiguration module (`AsyncKafkaStreamsAutoConfiguration`) activated by `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

**Beans created automatically:**

| Bean | Condition | Description |
|---|---|---|
| `asyncKafkaStreamsExecutor` (Executor) | `@ConditionalOnMissingBean(name="asyncKafkaStreamsExecutor")` | Fixed thread pool, `executorThreads` threads (default 32), named `async-kafka-streams-N`, daemon threads |
| `noopAsyncMetricsListener` (AsyncMetricsListener) | `@ConditionalOnMissingBean(AsyncMetricsListener.class)` | No-op listener when no other listener is registered |
| `micrometerAsyncMetricsListener` (AsyncMetricsListener) | `@ConditionalOnClass(MeterRegistry.class)` + `@ConditionalOnBean(MeterRegistry.class)` + `@ConditionalOnMissingBean(name="micrometerAsyncMetricsListener")` | Micrometer listener auto-registered when Micrometer is on classpath |

**Application property reference:**

```yaml
kafka:
  streams:
    async:
      executor-threads: 32             # Thread pool size for the auto-created executor
      processor-name: async-processor  # Tag on all metrics
      max-in-flight: 100               # Passed to options builder (informational; you set it in code)
      max-buffered-records: 10000
      output-drain-batch-size: 1000
      recovery-scan-max-records: 10000
      punctuate-interval: 100ms
      recovery-scan-interval: 5s
      store-commit-barrier-delay: 1s
      correctness-mode: STORE_FIRST_DEFERRED_SUBMIT
```

> These properties drive the `asyncKafkaStreamsExecutor` bean and the Micrometer listener. They do **not** auto-wire your `AsyncProcessorOptions`; you still construct that manually in your topology `@Bean`. Inject the executor and listener beans into your `@Bean` method as parameters.

**Override the executor:**

```java
@Bean(name = "asyncKafkaStreamsExecutor")
public Executor myCustomExecutor() {
    return Executors.newVirtualThreadPerTaskExecutor(); // Java 21 virtual threads
}
```

---

## 15. Operations Guide

### 15.1 Alerting

Set up alerts for the following conditions:

| Signal | Threshold | Meaning |
|---|---|---|
| `pending.store.entries` growing monotonically | > 5 min trend | Async calls are failing faster than succeeding; external system may be down |
| `output.store.entries` growing monotonically | > 5 min trend | Stream thread is not running punctuate; check stream thread liveness |
| `inflight` pinned at `maxInFlight` | Sustained > 5 min | Executor or external system is fully saturated |
| `backpressure.events` rate | > 0 sustained | Increase `maxBufferedRecords` or reduce `maxInFlight` |
| `records.retried` per second | Spike | External system degraded; investigate connectivity |
| `records.skipped` count | > 0 | Data loss (check if intentional via DROP/SKIP strategy) |
| `records.failed` without `records.retried` | > 0 | `maxAttempts=1` or exhausted retries and SKIP strategy → silent data loss |

### 15.2 Tuning Rules

```
maxInFlight         ≤  downstream safe parallelism (concurrent connections, DB pool size, rate limit)
maxBufferedRecords  ≥  maxInFlight × 10  (room for waiting-by-key items and recovery scan batches)
outputDrainBatchSize:  high enough to drain all completions within one punctuateInterval cycle
                        low enough not to monopolize the stream thread's punctuation time slot
                        → start at max(maxInFlight × 2, 1000)
recoveryScanMaxRecords: high enough to recover all pending records at restart
                        low enough that the scan does not stall the stream thread for >100ms
                        → start at 10,000; tune down if `scanPendingStore` latency is high
punctuateInterval:  100ms is a good default; 20-50ms for lower latency; 
                    increase for lower stream thread overhead at high throughput
storeCommitBarrierDelay: 1-2s for STORE_FIRST_DEFERRED_SUBMIT; 
                         must be >> your typical Kafka commit interval (commit.interval.ms / 1000)
```

### 15.3 Scaling

**Vertical scaling (first option):**
- Increase `maxInFlight` to saturate available network/connection bandwidth.
- Increase `executor-threads` if using a fixed thread pool (Java 21 virtual threads are better for I/O).
- Increase Kafka consumer fetch size to keep the input queue full.

**Horizontal scaling (partition-level):**
- More partitions = more task instances = more stream threads = more parallel processors.
- Each processor instance has its own state stores, so there is no cross-instance contention.
- Rebalances are more expensive with large pending stores (RocksDB restore from changelog topic).

**OpenShift / Kubernetes:**
- Scale vertically first (JVM virtual threads are very efficient for I/O-bound parallelism).
- Use `Ordering.KEY` if you have per-entity ordering requirements.
- Keep idempotency enabled in downstream systems when scaling horizontally (more rebalances = more duplicate external calls during recovery).

---

## 16. Developer Internals

This section is for contributors and advanced users who need to understand the internal architecture.

### 16.1 Module Map

```
kafka.streams.async/
│
├── AsyncProcessor.java              — Full Processor<KIn,VIn,KOut,VOut> implementation
├── AsyncFixedKeyProcessor.java      — Full FixedKeyProcessor implementation (same logic, different type bounds)
├── AsyncProcessorSupplier.java      — ProcessorSupplier factory
├── AsyncProcessorSuppliers.java     — Factory: fromHandler() and wrapValues()
├── AsyncProcessorOptions.java       — Builder-style configuration
│
├── AsyncRecordHandler.java          — User-facing: the async handler interface
├── AsyncRecordContext.java          — Passed to handler: recordId, attempt, metadata
├── AsyncOutput.java                 — Output record produced by handler
│
├── AsyncPendingRecord.java          — Durable pending-work record (stored in pendingStore)
├── AsyncPendingRecordSerde.java     — JSON serialization for AsyncPendingRecord
├── AsyncHeader.java                 — Serializable header representation
├── AsyncOutputRecord.java           — Durable output record (stored in outputStore)
├── AsyncOutputRecordSerde.java      — JSON serialization for AsyncOutputRecord
│
├── AsyncStores.java                 — StoreBuilder factories for pending + output stores
│
├── Ordering.java                    — KEY / UNORDERED
├── BackpressureStrategy.java        — FAIL / BLOCK / DROP
├── ErrorStrategy.java               — FAIL_TASK / SKIP
├── CorrectnessMode.java             — FAST_IN_MEMORY_SUBMIT / STORE_FIRST_DEFERRED_SUBMIT / IDEMPOTENT_EXTERNAL_EFFECT
├── RecordIdStrategy.java            — topicPartitionOffset() and custom strategies
├── StateStoreAccessPolicy.java      — DISALLOW / ALLOW_UNSAFE
│
├── AsyncErrorHandler.java           — User-facing: error callback
├── AsyncMetricsListener.java        — User-facing: metrics event callback
├── AsyncMetricsSnapshot.java        — Immutable gauge snapshot passed to onSnapshot
├── MicrometerAsyncMetricsListener.java — Micrometer adapter
├── CountingAsyncMetrics.java        — Internal AtomicLong counters
│
├── CapturingFixedKeyProcessorContext.java — Buffered context for wrapper API
│
└── spring/
    ├── AsyncKafkaStreamsAutoConfiguration.java  — Spring Boot autoconfiguration
    └── AsyncKafkaStreamsProperties.java         — @ConfigurationProperties
```

### 16.2 Internal State Machines

Each `AsyncProcessor` manages these concurrent data structures:

```
scheduledIds       : Set<String>          — canonical set of all tracked record IDs
                                             added at enqueueIfNotInFlight,
                                             removed at finishRecord
                                             guards against double-enqueue

inFlightIds        : Set<String>          — records currently executing on the executor
                                             added at submit(), removed at drainCompleted()

ready              : Queue<WorkItem>      — items ready to submit (not yet acquired a permit)

completed          : ConcurrentLinkedQueue  — results from async callbacks, 
                                             cross-thread: written by executor threads,
                                             drained by stream thread

waitingByKey       : Map<key, Queue>      — KEY ordering: records blocked behind an 
                                             active record with the same key

activeKeys         : Set<Object>          — KEY ordering: keys currently in-flight
                                             (exactly one record per key max)

activeKeyByRecordId : Map<id,key>        — reverse lookup: which key owns a given pending ID
                                             used to release key lane at finishKeyIfNeeded

permits            : Semaphore(maxInFlight) — bounds in-flight count
```

**Buffer accounting:** `scheduledIds.size()` is the single source of truth for `bufferedRecords()`. Sub-queues (`ready`, `inFlightIds`, `completed`, `waitingByKey`) are implementation details and must **not** be counted separately — doing so would cause multi-counting and trigger false backpressure.

### 16.3 Thread Safety Model

| Data structure | Written by | Read by | Safety mechanism |
|---|---|---|---|
| `ready` | Stream thread (enqueueIfNotInFlight, finishKeyIfNeeded, handleFailure) | Stream thread (drainReady) | Single-threaded (all on stream thread) — `ArrayDeque` is fine |
| `completed` | **Executor threads** (async callbacks) + stream thread (synchronous executor in tests) | Stream thread (drainCompleted) | `ConcurrentLinkedQueue` |
| `inFlightIds`, `scheduledIds`, `activeKeys`, `waitingByKey`, `activeKeyByRecordId` | Stream thread | Stream thread + read-only check from executor threads (not needed normally) | `ConcurrentHashMap` / `ConcurrentHashMap.newKeySet()` |
| `pendingStore`, `outputStore` | Stream thread only | Stream thread only | — |
| `closed` | Stream thread (`close()`) | Stream thread + executor threads (guard in lambdas) | `volatile` |

### 16.4 The `drainReady` / `drainCompleted` Loop

`drainReady(now)` processes up to `ready.size()` items (snapshotted at entry):

```java
int iterations = ready.size();   // snapshot — items added during this loop are NOT picked up
while (iterations-- > 0 && permits.tryAcquire()) {
    WorkItem item = ready.poll();
    if (item.pending().notBeforeEpochMs() > now) {
        ready.offer(item); permits.release(); continue;  // re-queue for later
    }
    if (inFlightIds.contains(item.pending().recordId()) || hasStoredOutput(item.pending().recordId())) {
        permits.release(); continue;  // stale/duplicate, skip
    }
    if (KEY ordering: key is active by someone else) {
        waitingByKey.offer(item); permits.release(); continue;
    }
    // Claim key ownership and submit
    activeKeys.add(key);
    activeKeyByRecordId.put(id, key);
    submit(item);
}
```

Because `iterations` is a snapshot, items added to `ready` mid-loop (e.g. retry items from `handleFailure`, or promoted waiting items from `finishKeyIfNeeded`) are processed in the **next** drain call, not the current one.

`scanPersistentStores` compensates for this for synchronous executors (e.g. `TopologyTestDriver`) by running a safety loop:

```java
int safetyLimit = options.maxAttempts() + 2;
for (int round = 0; round < safetyLimit; round++) {
    if (completed.isEmpty() && ready.isEmpty()) break;
    drainCompleted();
    drainStoredOutputs();
    drainReady(System.currentTimeMillis());
}
```

### 16.5 Key-Ordering Algorithm

```
enqueueIfNotInFlight(item):
  if scheduledIds contains id → return (already tracked)
  scheduledIds.add(id)
  if KEY ordering AND activeKeys contains key AND key is NOT already owned by this id:
    waitingByKey[key].offer(item)      // queue it, do not offer to ready
    return
  ready.offer(item)

drainReady:
  for item in ready:
    if KEY ordering AND activeKeys contains key AND key != owned by this id:
      waitingByKey[key].offer(item)    // conflict appeared since enqueue, re-queue
      continue
    activeKeys.add(key)
    activeKeyByRecordId.put(id, key)
    submit(item)

finishKeyIfNeeded(key):     // called from finishRecord at end of successful processing
  activeKeys.remove(key)    // ALWAYS release first
  next = waitingByKey[key].poll()
  if next != null:
    ready.offer(next)       // promote next waiter to ready queue
  else if queue empty:
    waitingByKey.remove(key, emptyQueue)
```

**Why `activeKeys.remove(key)` must come before `ready.offer(next)`:**
If the key were still in `activeKeys` when `next` is offered to `ready`, `drainReady` would see a conflict and move `next` back to `waitingByKey` — an infinite loop.

### 16.6 Buffer Counting

```java
private int bufferedRecords() {
    return scheduledIds.size();
}
```

`scheduledIds` is the canonical buffer:
- Added in `enqueueIfNotInFlight` (exactly once per record, even if record visits `waitingByKey` → `ready` → `inFlightIds` → `completed`).
- Removed in `finishRecord` (called after final forwarding or after permanent failure/drop).

**Never count sub-queues.** A record in `ready` is also in `scheduledIds`. A record in `inFlightIds` is also in `scheduledIds`. Counting both would double-count.

### 16.7 Recovery on Restart

`scanPersistentStores` / `scanPendingStore` runs every `recoveryScanInterval` (default 5 s):

1. Iterates over all entries in `pendingStore` (up to `recoveryScanMaxRecords`).
2. Skips entries already in `inFlightIds` (currently being processed).
3. Skips entries that already have stored output (completed but not yet forwarded — those are picked up by `drainStoredOutputs`).
4. Skips entries where `notBeforeEpochMs > now` (backoff not yet expired).
5. Calls `enqueueIfNotInFlight` for the rest → adds to `ready`.

This handles:
- **Full restart**: All pending records are recovered from RocksDB and re-submitted.
- **Crashed async work**: Work that was in flight when the JVM died has no in-memory entry in `inFlightIds` after restart, so it is re-submitted.
- **Rebalance**: Same as full restart for the newly assigned partitions.

### 16.8 The Responsive-Style Wrapper (AsyncFixedKeyProcessor)

`AsyncFixedKeyProcessor` wraps a user-provided `FixedKeyProcessorSupplier`. On each async execution:

1. Creates a new instance of the delegate processor via the supplier.
2. Wraps the real `FixedKeyProcessorContext` in a `CapturingFixedKeyProcessorContext`.
3. Calls `delegate.init(capturingContext)` and `delegate.process(record)`.
4. Collects all `context.forward(...)` calls captured by `CapturingFixedKeyProcessorContext`.
5. Returns the captured outputs as the `CompletionStage` result.

`CapturingFixedKeyProcessorContext` intercepts:
- `forward(record)` / `forward(record, childName)` → stores in a list.
- `getStateStore(name)` → throws `IllegalStateException` unless `ALLOW_UNSAFE`.
- All other context methods → either delegate to the real context or return safe defaults.

This ensures that all actual Kafka Streams API calls (`forward`, state store access) only happen from the stream thread during `drainStoredOutputs`, never from the async executor.

---

## 17. Build, Test, and CI

**Requirements:**
- Java 21+
- Maven 3.8+
- Docker (for integration tests via Testcontainers)

**Build and unit tests:**

```bash
mvn clean test
```

**Full build including integration tests:**

```bash
mvn clean install
```

Integration tests run via Maven Failsafe plugin. They spin up a real Kafka broker using Testcontainers (`apache/kafka:4.2.0`). They require Docker to be running.

**Skipping integration tests (faster development cycle):**

```bash
mvn clean test -DskipITs
```

**Test inventory:**

| Test class | Type | Description |
|---|---|---|
| `AsyncProcessorTest` | Unit | Core processor behavior: `TopologyTestDriver`, no real Kafka. Tests: successful processing, retry-then-succeed, exhausted retries. |
| `WrapperTest` | Unit | Wrapper API behavior: `TopologyTestDriver`. Tests: captured forwarding, state store access enforcement. |
| `CorrectnessIT` | Integration | Real Kafka via Testcontainers. Tests: pending store crash recovery, key ordering (20 records, single key), concurrent distinct-key processing. |
| `ErrorHandlingIT` | Integration | Real Kafka. Tests: transient failure + retry, `SKIP` strategy, `BLOCK` backpressure. |
| `ThroughputIT` | Integration | Real Kafka. Proves `>= 4×` speedup of async vs synchronous topology with 200 records and 20 ms simulated I/O. |

**Key integration test: key ordering**

```
CorrectnessIT.keyOrderingIsPreservedEndToEnd:
  - 20 records, all with the same key
  - async handler sleeps random 50–150 ms
  - assertions: all 20 records forwarded, in the original input order
```

**Key integration test: throughput**

```
ThroughputIT.asyncThroughputSignificantlyExceedsSynchronous:
  - 200 records, 20 ms I/O each
  - synchronous baseline: ~5200 ms
  - async (50 in-flight): ~1000 ms
  - assertion: speedup >= 4.0×
```

---

## 18. FAQ

**Q: Do I need both stores (pending + output)?**

A: For crash recovery, you need at least the pending store. The output store adds durable completion: without it, if the JVM dies after a successful HTTP call but before `context.forward(...)`, the result is lost and the call will be retried. With only the pending store, that retry is safe only if the external system is idempotent. With both stores, the result is persisted before forwarding, so no re-execution happens. Omitting both stores gives the best performance but no crash recovery at all — acceptable only for completely idempotent, non-critical enrichment.

**Q: What executor should I use?**

A: For I/O-bound handlers (HTTP calls, DB queries): **Java 21 virtual threads** (`Executors.newVirtualThreadPerTaskExecutor()`). They are extremely lightweight and scale to millions of concurrent blocked calls.

For CPU-bound handlers: **fixed thread pool** sized to `Runtime.getRuntime().availableProcessors()`.

For testing (`TopologyTestDriver`): use a **direct executor** (`Runnable::run`) so all work completes synchronously without spawning threads.

**Q: Will this work with `processing.guarantee=exactly_once_v2`?**

A: Yes. The processor uses standard `KeyValueStore` and `context.forward()`, which are both transaction-aware. The exactly-once guarantee applies to Kafka state store writes and output topic produces. It does not extend to external HTTP calls.

**Q: Can I have multiple async processors in the same topology?**

A: Yes. Each processor instance has its own options, stores, and queues. Use distinct `pendingStoreName`, `outputStoreName`, and `processorName` for each.

**Q: What happens during a rebalance?**

A: In-flight work is abandoned (the JVM continues running but the task is no longer assigned the partition). After the rebalance completes and the partition is assigned to a new task (possibly the same JVM), the pending store is restored from the changelog topic and `scanPersistentStores` re-submits the unfinished records. The external system will receive retries — hence the idempotency requirement.

**Q: Can the wrapper API access Kafka state stores?**

A: Not safely by default. `StateStoreAccessPolicy.DISALLOW` prevents it. You can opt into `ALLOW_UNSAFE` if you have proven your processor only reads (not writes), the store only uses `get()` (not `put()`/`delete()`), and you accept the risk of data races. In practice it is better to read the state you need on the stream thread and pass it into the handler as a captured variable.

**Q: What is `storeCommitBarrierDelay` and why does it default to 1 second?**

A: When `STORE_FIRST_DEFERRED_SUBMIT` is used, the processor writes the pending record and calls `context.commit()`. The Kafka Streams `ProcessorContext.commit()` is a hint, not a blocking call — the actual commit happens asynchronously. The `storeCommitBarrierDelay` (default 1 s) is a time buffer: async work is only submitted from the recovery scan after this delay has passed since the pending record was written. This makes it very likely that the pending record was committed to the changelog before the external call goes out. There is no formal guarantee because the public Processor API has no commit callback.

**Q: How do I test my topology with `TopologyTestDriver`?**

A: Use `Runnable::run` as the executor (direct/synchronous). Set `punctuateInterval` and `recoveryScanInterval` to small values. Call `driver.advanceWallClockTime(Duration.ofSeconds(X))` to trigger punctuation. The `safetyLimit` drain loop in `scanPersistentStores` handles the complete retry chain synchronously.

```java
Executor directExecutor = Runnable::run;

AsyncProcessorOptions<...> options = AsyncProcessorOptions.builder(directExecutor)
    .pendingStoreName("test-pending")
    .outputStoreName("test-output")
    .punctuateInterval(Duration.ofMillis(10))
    .recoveryScanInterval(Duration.ofMillis(50))
    .storeCommitBarrierDelay(Duration.ZERO)
    .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
    .maxAttempts(3)
    .initialBackoff(Duration.ZERO)
    .maxBackoff(Duration.ZERO)
    .build();

// In test:
inputTopic.pipeInput("key", "value");
driver.advanceWallClockTime(Duration.ofSeconds(2));
assertThat(outputTopic.readValue()).isEqualTo("expected");
```

