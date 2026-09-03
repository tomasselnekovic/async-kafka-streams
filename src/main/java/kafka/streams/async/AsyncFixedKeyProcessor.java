package kafka.streams.async;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async wrapper used by KStream.processValues(...).
 *
 * <p>Implementation note: the wrapped delegate is instantiated per input record. This is deliberate:
 * it prevents sharing a normal Kafka Streams processor instance across many async worker threads.
 * Stateful delegate processors can still be used for local in-memory setup, but Kafka Streams state-store
 * access from the worker thread is blocked by default.</p>
 */
final class AsyncFixedKeyProcessor<K, VIn, VOut> implements FixedKeyProcessor<K, VIn, VOut> {
    private static final Logger log = LoggerFactory.getLogger(AsyncFixedKeyProcessor.class);

    private final FixedKeyProcessorSupplier<K, VIn, VOut> delegateSupplier;
    private final AsyncProcessorOptions<K, VIn, K, VOut> options;
    private final CountingAsyncMetrics counters = new CountingAsyncMetrics();

    private FixedKeyProcessorContext<K, VOut> context;
    private KeyValueStore<String, AsyncPendingRecord<K, VIn>> pendingStore;
    private KeyValueStore<String, AsyncOutputRecord<K, VOut>> outputStore;

    private final Semaphore permits;
    private final Queue<WorkItem<K, VIn>> ready = new ArrayDeque<>();
    private final Queue<Completed<K, VIn, VOut>> completed = new ConcurrentLinkedQueue<>();
    private final Map<Object, Queue<WorkItem<K, VIn>>> waitingByKey = new ConcurrentHashMap<>();
    private final Map<String, Object> activeKeyByRecordId = new ConcurrentHashMap<>();
    private final Set<Object> activeKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightIds = ConcurrentHashMap.newKeySet();
    private final Set<String> scheduledIds = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    AsyncFixedKeyProcessor(FixedKeyProcessorSupplier<K, VIn, VOut> delegateSupplier,
                           AsyncProcessorOptions<K, VIn, K, VOut> options) {
        this.delegateSupplier = Objects.requireNonNull(delegateSupplier, "delegateSupplier");
        this.options = Objects.requireNonNull(options, "options");
        this.permits = new Semaphore(options.maxInFlight());
    }

    @Override
    public void init(FixedKeyProcessorContext<K, VOut> context) {
        this.context = context;
        if (options.durablePendingStoreEnabled()) {
            this.pendingStore = context.getStateStore(options.pendingStoreName());
        }
        if (options.durableOutputStoreEnabled()) {
            this.outputStore = context.getStateStore(options.outputStoreName());
        }
        context.schedule(options.punctuateInterval(), PunctuationType.WALL_CLOCK_TIME, this::onPunctuate);
        context.schedule(options.recoveryScanInterval(), PunctuationType.WALL_CLOCK_TIME, this::scanPersistentStores);
        emitSnapshot();
    }

    @Override
    public void process(FixedKeyRecord<K, VIn> fixedRecord) {
        Record<K, VIn> record = toRecord(fixedRecord);
        Optional<RecordMetadata> metadata = context.recordMetadata();
        String recordId = options.recordIdStrategy().id(record, metadata);
        counters.received.incrementAndGet();
        options.metricsListener().onRecordReceived(options.processorName(), recordId);

        if (!ensureCapacity(record, recordId)) {
            return;
        }

        if (pendingStore != null) {
            AsyncPendingRecord<K, VIn> existing = pendingStore.get(recordId);
            if (existing != null) {
                if (!hasStoredOutput(recordId)) {
                    enqueueIfNotInFlight(new WorkItem<>(existing, metadata));
                }
                drainReady(System.currentTimeMillis());
                return;
            }

            long notBefore = initialSubmitNotBeforeEpochMs();
            AsyncPendingRecord<K, VIn> pending = AsyncPendingRecord.from(recordId, record, 1, notBefore);
            pendingStore.put(recordId, pending);

            if (options.correctnessMode() == CorrectnessMode.FAST_IN_MEMORY_SUBMIT) {
                enqueueIfNotInFlight(new WorkItem<>(pending, metadata));
            } else {
                context.commit();
            }
        } else {
            AsyncPendingRecord<K, VIn> pending = AsyncPendingRecord.from(recordId, record, 1, 0L);
            enqueueIfNotInFlight(new WorkItem<>(pending, metadata));
        }
        drainReady(System.currentTimeMillis());
    }

    private Record<K, VIn> toRecord(FixedKeyRecord<K, VIn> r) {
        return new Record<>(r.key(), r.value(), r.timestamp(), r.headers());
    }

    private FixedKeyRecord<K, VOut> toFixedKeyRecord(AsyncOutputRecord<K, VOut> out) {
        RecordHeaders headers = new RecordHeaders();
        if (out.headers() != null) {
            out.headers().forEach(h -> headers.add(h.key(), h.value()));
        }
        return InternalFixedKeyRecordFactory.create(new Record<>(out.key(), out.value(), out.timestamp(), headers));
    }

    private long initialSubmitNotBeforeEpochMs() {
        if (options.correctnessMode() == CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT
                || options.correctnessMode() == CorrectnessMode.IDEMPOTENT_EXTERNAL_EFFECT) {
            return System.currentTimeMillis() + options.storeCommitBarrierDelay().toMillis();
        }
        return 0L;
    }

    private boolean ensureCapacity(Record<K, VIn> record, String recordId) {
        if (bufferedRecords() < options.maxBufferedRecords()) return true;
        counters.backpressure.incrementAndGet();
        options.metricsListener().onBackpressure(options.processorName(), recordId, options.backpressureStrategy());
        if (options.backpressureStrategy() == BackpressureStrategy.DROP) {
            options.errorHandler().onFailure(record, new StreamsException("Async processor buffer is full"), 0);
            counters.skipped.incrementAndGet();
            options.metricsListener().onRecordSkipped(options.processorName(), recordId);
            return false;
        }
        if (options.backpressureStrategy() == BackpressureStrategy.BLOCK) {
            long deadline = System.currentTimeMillis() + options.backpressureBlockTimeout().toMillis();
            while (System.currentTimeMillis() < deadline && bufferedRecords() >= options.maxBufferedRecords()) {
                onPunctuate(System.currentTimeMillis());
                try { Thread.sleep(Math.min(25L, Math.max(1L, options.punctuateInterval().toMillis()))); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new StreamsException("Interrupted", e); }
            }
            if (bufferedRecords() < options.maxBufferedRecords()) return true;
        }
        throw new StreamsException("Async processor buffer is full: " + options.maxBufferedRecords());
    }

    private int bufferedRecords() {
        // scheduledIds is the canonical set: every tracked record is in it from enqueue until
        // finishRecord. The sub-queues (ready, inFlightIds, completed, waitingByKey) are
        // implementation details and must NOT be counted separately to avoid multi-counting.
        return scheduledIds.size();
    }

    private void onPunctuate(long timestamp) {
        if (closed) return;
        drainStoredOutputs();
        drainCompleted();
        drainStoredOutputs();
        drainReady(System.currentTimeMillis());
        emitSnapshot();
        context.commit();
    }

    private void scanPersistentStores(long timestamp) {
        if (closed || pendingStore == null) return;
        drainStoredOutputs();
        long now = System.currentTimeMillis();
        int scanned = 0;
        try (KeyValueIterator<String, AsyncPendingRecord<K, VIn>> it = pendingStore.all()) {
            while (it.hasNext() && scanned++ < options.recoveryScanMaxRecords()) {
                KeyValue<String, AsyncPendingRecord<K, VIn>> e = it.next();
                AsyncPendingRecord<K, VIn> pending = e.value;
                if (pending == null || inFlightIds.contains(pending.recordId())) continue;
                if (hasStoredOutput(pending.recordId())) continue;
                if (pending.notBeforeEpochMs() <= now) enqueueIfNotInFlight(new WorkItem<>(pending, Optional.empty()));
            }
        }
        drainReady(now);
        // Fully drain the pipeline for synchronous/direct executors (e.g. TopologyTestDriver tests).
        // With a real async executor these calls are cheap no-ops when nothing has completed yet.
        // The loop handles retries with zero backoff: failure → retry-in-ready → success → forward.
        // Use a fresh System.currentTimeMillis() each round: handleFailure() sets notBeforeEpochMs to
        // currentTimeMillis()+backoff, so reusing a pre-loop timestamp can make notBefore > now even
        // when backoff is zero (millisecond boundary between the two calls).
        int safetyLimit = options.maxAttempts() + 2;
        for (int round = 0; round < safetyLimit; round++) {
            if (completed.isEmpty() && ready.isEmpty()) break;
            drainCompleted();
            drainStoredOutputs();
            drainReady(System.currentTimeMillis());
        }
        drainCompleted();
        drainStoredOutputs();
        emitSnapshot();
    }

    private boolean hasStoredOutput(String recordId) {
        if (outputStore == null) return false;
        String from = AsyncOutputRecord.prefix(recordId);
        String to = prefixUpperBound(from);
        try (KeyValueIterator<String, AsyncOutputRecord<K, VOut>> iterator = outputStore.range(from, to)) {
            return iterator.hasNext();
        }
    }

    private List<AsyncOutputRecord<K, VOut>> loadStoredOutputsBatch() {
        List<AsyncOutputRecord<K, VOut>> outputs = new ArrayList<>();
        if (outputStore == null) return outputs;
        try (KeyValueIterator<String, AsyncOutputRecord<K, VOut>> iterator = outputStore.all()) {
            while (iterator.hasNext() && outputs.size() < options.outputDrainBatchSize()) {
                AsyncOutputRecord<K, VOut> value = iterator.next().value;
                if (value != null) outputs.add(value);
            }
        }
        outputs.sort(Comparator.comparing(AsyncOutputRecord<K, VOut>::recordId).thenComparingInt(AsyncOutputRecord::sequence));
        return outputs;
    }

    private void enqueueIfNotInFlight(WorkItem<K, VIn> item) {
        if (inFlightIds.contains(item.pending().recordId()) || scheduledIds.contains(item.pending().recordId())) return;
        scheduledIds.add(item.pending().recordId());
        Object key = keyFor(item.pending());
        if (options.ordering() == Ordering.KEY && activeKeys.contains(key)
                && !key.equals(activeKeyByRecordId.get(item.pending().recordId()))) {
            waitingByKey.computeIfAbsent(key, ignored -> new ArrayDeque<>()).offer(item);
            return;
        }
        ready.offer(item);
    }

    private void drainCompleted() {
        Completed<K, VIn, VOut> done;
        while ((done = completed.poll()) != null) {
            permits.release();
            inFlightIds.remove(done.item().pending().recordId());
            if (done.error() == null) {
                counters.succeeded.incrementAndGet();
                options.metricsListener().onRecordSucceeded(options.processorName(), done.item().pending().recordId());
                handleSuccess(done.item(), done.outputs());
            } else {
                counters.failed.incrementAndGet();
                options.metricsListener().onRecordFailed(options.processorName(), done.item().pending().recordId(),
                        done.item().pending().attempt(), done.error());
                handleFailure(done.item(), done.error());
            }
        }
    }

    private void handleSuccess(WorkItem<K, VIn> item, Collection<AsyncOutput<K, VOut>> outputs) {
        String recordId = item.pending().recordId();
        Record<K, VIn> original = item.pending().toRecord();
        if (outputStore != null) {
            Collection<AsyncOutput<K, VOut>> safeOutputs = outputs == null ? List.of() : outputs;
            if (safeOutputs.isEmpty()) {
                AsyncOutputRecord<K, VOut> terminal = AsyncOutputRecord.terminal(recordId);
                outputStore.put(terminal.storeKey(), terminal);
                counters.terminal.incrementAndGet();
            } else {
                int seq = 0;
                for (AsyncOutput<K, VOut> output : safeOutputs) {
                    outputStore.put(AsyncOutputRecord.storeKey(recordId, seq),
                            AsyncOutputRecord.from(recordId, seq++, output, original.timestamp(), original.headers()));
                }
            }
            return;
        }
        forward(outputs, original, recordId);
        if (pendingStore != null) pendingStore.delete(recordId);
        finishRecord(recordId);
    }

    private void drainStoredOutputs() {
        if (outputStore == null) return;
        for (AsyncOutputRecord<K, VOut> out : loadStoredOutputsBatch()) {
            if (!out.terminalMarker()) {
                FixedKeyRecord<K, VOut> record = toFixedKeyRecord(out);
                if (out.childName() == null || out.childName().isBlank()) context.forward(record);
                else context.forward(record, out.childName());
                counters.forwarded.incrementAndGet();
                options.metricsListener().onRecordForwarded(options.processorName(), out.recordId(), out.sequence());
            }
            outputStore.delete(out.storeKey());
            if (!hasStoredOutput(out.recordId())) {
                if (pendingStore != null) pendingStore.delete(out.recordId());
                finishRecord(out.recordId());
            }
        }
    }

    private void forward(Collection<AsyncOutput<K, VOut>> outputs, Record<K, VIn> original, String recordId) {
        if (outputs == null) return;
        int seq = 0;
        for (AsyncOutput<K, VOut> output : outputs) {
            long timestamp = output.timestamp() == null ? original.timestamp() : output.timestamp();
            FixedKeyRecord<K, VOut> out = InternalFixedKeyRecordFactory.create(new Record<>(output.key(), output.value(), timestamp, original.headers()));
            if (output.childName() == null || output.childName().isBlank()) context.forward(out);
            else context.forward(out, output.childName());
            counters.forwarded.incrementAndGet();
            options.metricsListener().onRecordForwarded(options.processorName(), recordId, seq++);
        }
    }

    private void handleFailure(WorkItem<K, VIn> item, Throwable error) {
        AsyncPendingRecord<K, VIn> pending = item.pending();
        if (pending.attempt() < options.maxAttempts()) {
            int nextAttempt = pending.attempt() + 1;
            long notBefore = System.currentTimeMillis() + backoff(pending.attempt());
            AsyncPendingRecord<K, VIn> retry = pending.retry(nextAttempt, notBefore, error);
            if (pendingStore != null) pendingStore.put(retry.recordId(), retry);
            counters.retried.incrementAndGet();
            options.metricsListener().onRecordRetried(options.processorName(), retry.recordId(), nextAttempt, notBefore);
            ready.offer(new WorkItem<>(retry, item.recordMetadata()));
            return;
        }
        options.errorHandler().onFailure(pending.toRecord(), error, pending.attempt());
        if (pendingStore != null) pendingStore.delete(pending.recordId());
        finishRecord(pending.recordId());
        if (options.errorStrategy() == ErrorStrategy.FAIL_TASK) {
            throw new StreamsException("Async processing failed after " + pending.attempt() + " attempts", error);
        }
        counters.skipped.incrementAndGet();
        options.metricsListener().onRecordSkipped(options.processorName(), pending.recordId());
        log.warn("Skipping record after {} failed attempts: recordId={}, key={}", pending.attempt(), pending.recordId(), pending.key(), error);
    }

    private long backoff(int attempt) {
        long raw = options.initialBackoff().toMillis() * (1L << Math.min(20, attempt - 1));
        return Math.min(raw, options.maxBackoff().toMillis());
    }

    private void finishRecord(String recordId) {
        scheduledIds.remove(recordId);
        Object key = activeKeyByRecordId.remove(recordId);
        if (key != null) finishKeyIfNeeded(key);
    }

    private void finishKeyIfNeeded(Object key) {
        if (options.ordering() != Ordering.KEY) return;
        // Always release key ownership first so drainReady can re-acquire it for the next record.
        activeKeys.remove(key);
        Queue<WorkItem<K, VIn>> queue = waitingByKey.get(key);
        WorkItem<K, VIn> next = queue == null ? null : queue.poll();
        if (next == null) {
            if (queue != null && queue.isEmpty()) waitingByKey.remove(key, queue);
        } else ready.offer(next);
    }

    private void drainReady(long now) {
        int iterations = ready.size();
        while (iterations-- > 0 && permits.tryAcquire()) {
            WorkItem<K, VIn> item = ready.poll();
            if (item == null) { permits.release(); return; }
            if (item.pending().notBeforeEpochMs() > now) { ready.offer(item); permits.release(); continue; }
            if (inFlightIds.contains(item.pending().recordId()) || hasStoredOutput(item.pending().recordId())) { permits.release(); continue; }
            if (options.ordering() == Ordering.KEY) {
                Object key = keyFor(item.pending());
                Object owner = activeKeyByRecordId.get(item.pending().recordId());
                if (activeKeys.contains(key) && !key.equals(owner)) {
                    waitingByKey.computeIfAbsent(key, ignored -> new ArrayDeque<>()).offer(item);
                    permits.release();
                    continue;
                }
                activeKeys.add(key);
                activeKeyByRecordId.put(item.pending().recordId(), key);
            }
            submit(item);
        }
    }

    private void submit(WorkItem<K, VIn> item) {
        inFlightIds.add(item.pending().recordId());
        counters.submitted.incrementAndGet();
        options.metricsListener().onRecordSubmitted(options.processorName(), item.pending().recordId(), item.pending().attempt());
        try {
            options.executor().execute(() -> {
                try {
                    FixedKeyProcessor<K, VIn, VOut> delegate = delegateSupplier.get();
                    CapturingFixedKeyProcessorContext<K, VOut> capturingContext = new CapturingFixedKeyProcessorContext<>(
                            context, item.pending().recordId(), options.stateStoreAccessPolicy());
                    delegate.init(capturingContext);
                    Record<K, VIn> input = item.pending().toRecord();
                    delegate.process(InternalFixedKeyRecordFactory.create(new Record<>(input.key(), input.value(), input.timestamp(), input.headers())));
                    delegate.close();
                    completed.offer(new Completed<>(item, capturingContext.capturedOutputs(), null));
                } catch (Throwable t) {
                    completed.offer(new Completed<>(item, null, t));
                }
            });
        } catch (Throwable t) {
            completed.offer(new Completed<>(item, null, t));
        }
    }

    private Object keyFor(AsyncPendingRecord<K, VIn> pending) { return pending.key() == null ? NullKey.INSTANCE : pending.key(); }

    private void emitSnapshot() {
        long pendingEntries = pendingStore == null ? -1 : pendingStore.approximateNumEntries();
        long outputEntries = outputStore == null ? -1 : outputStore.approximateNumEntries();
        options.metricsListener().onSnapshot(options.processorName(), new AsyncMetricsSnapshot(
                counters.received.get(), counters.submitted.get(), counters.succeeded.get(), counters.failed.get(),
                counters.retried.get(), counters.skipped.get(), counters.forwarded.get(), counters.terminal.get(),
                counters.backpressure.get(), inFlightIds.size(), scheduledIds.size(), ready.size(), completed.size(),
                pendingEntries, outputEntries));
    }

    private static String prefixUpperBound(String prefix) {
        if (prefix.isEmpty()) return Character.toString(Character.MAX_VALUE);
        char[] chars = prefix.toCharArray();
        for (int i = chars.length - 1; i >= 0; i--) {
            if (chars[i] != Character.MAX_VALUE) { chars[i]++; return new String(chars, 0, i + 1); }
        }
        return prefix + Character.MAX_VALUE;
    }

    @Override public void close() { closed = true; emitSnapshot(); }

    private enum NullKey { INSTANCE }
    private record WorkItem<K, V>(AsyncPendingRecord<K, V> pending, Optional<RecordMetadata> recordMetadata) { }
    private record Completed<K, VIn, VOut>(WorkItem<K, VIn> item, Collection<AsyncOutput<K, VOut>> outputs, Throwable error) { }
}
