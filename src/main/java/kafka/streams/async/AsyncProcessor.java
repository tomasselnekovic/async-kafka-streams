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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AsyncProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

    private final AsyncRecordHandler<KIn, VIn, KOut, VOut> handler;
    private final AsyncProcessorOptions<KIn, VIn, KOut, VOut> options;
    private final CountingAsyncMetrics counters = new CountingAsyncMetrics();

    private ProcessorContext<KOut, VOut> context;
    private KeyValueStore<String, AsyncPendingRecord<KIn, VIn>> pendingStore;
    private KeyValueStore<String, AsyncOutputRecord<KOut, VOut>> outputStore;

    private final Semaphore permits;
    private final Queue<WorkItem<KIn, VIn>> ready = new ArrayDeque<>();
    private final Queue<Completed<KIn, VIn, KOut, VOut>> completed = new ConcurrentLinkedQueue<>();
    private final Map<Object, Queue<WorkItem<KIn, VIn>>> waitingByKey = new ConcurrentHashMap<>();
    private final Map<String, Object> activeKeyByRecordId = new ConcurrentHashMap<>();
    private final Set<Object> activeKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightIds = ConcurrentHashMap.newKeySet();
    private final Set<String> scheduledIds = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    AsyncProcessor(AsyncRecordHandler<KIn, VIn, KOut, VOut> handler,
                   AsyncProcessorOptions<KIn, VIn, KOut, VOut> options) {
        this.handler = Objects.requireNonNull(handler, "handler");
        this.options = Objects.requireNonNull(options, "options");
        this.permits = new Semaphore(options.maxInFlight());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext<KOut, VOut> context) {
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
    public void process(Record<KIn, VIn> record) {
        Optional<RecordMetadata> metadata = context.recordMetadata();
        String recordId = options.recordIdStrategy().id(record, metadata);
        counters.received.incrementAndGet();
        options.metricsListener().onRecordReceived(options.processorName(), recordId);

        if (!ensureCapacity(record, recordId)) {
            return;
        }

        if (pendingStore != null) {
            AsyncPendingRecord<KIn, VIn> existing = pendingStore.get(recordId);
            if (existing != null) {
                if (!hasStoredOutput(recordId)) {
                    enqueueIfNotInFlight(new WorkItem<>(existing, metadata));
                }
                drainReady(System.currentTimeMillis());
                return;
            }

            long notBefore = initialSubmitNotBeforeEpochMs();
            AsyncPendingRecord<KIn, VIn> pending = AsyncPendingRecord.from(recordId, record, 1, notBefore);
            pendingStore.put(recordId, pending);

            if (options.correctnessMode() == CorrectnessMode.FAST_IN_MEMORY_SUBMIT) {
                enqueueIfNotInFlight(new WorkItem<>(pending, metadata));
            } else {
                // Ask Kafka Streams to commit the durable intent before we start the external side effect.
                // The public Processor API has no commit-completed callback, so the delayed scan is a
                // conservative approximation rather than a formal transaction boundary.
                context.commit();
            }
        } else {
            AsyncPendingRecord<KIn, VIn> pending = AsyncPendingRecord.from(recordId, record, 1, 0L);
            enqueueIfNotInFlight(new WorkItem<>(pending, metadata));
        }
        drainReady(System.currentTimeMillis());
    }

    private long initialSubmitNotBeforeEpochMs() {
        if (options.correctnessMode() == CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT
                || options.correctnessMode() == CorrectnessMode.IDEMPOTENT_EXTERNAL_EFFECT) {
            return System.currentTimeMillis() + options.storeCommitBarrierDelay().toMillis();
        }
        return 0L;
    }

    private boolean ensureCapacity(Record<KIn, VIn> record, String recordId) {
        if (bufferedRecords() < options.maxBufferedRecords()) {
            return true;
        }
        counters.backpressure.incrementAndGet();
        options.metricsListener().onBackpressure(options.processorName(), recordId, options.backpressureStrategy());

        if (options.backpressureStrategy() == BackpressureStrategy.DROP) {
            options.errorHandler().onFailure(record,
                    new StreamsException("Async processor buffer is full: " + options.maxBufferedRecords()), 0);
            counters.skipped.incrementAndGet();
            options.metricsListener().onRecordSkipped(options.processorName(), recordId);
            return false;
        }
        if (options.backpressureStrategy() == BackpressureStrategy.BLOCK) {
            long deadline = System.currentTimeMillis() + options.backpressureBlockTimeout().toMillis();
            while (System.currentTimeMillis() < deadline && bufferedRecords() >= options.maxBufferedRecords()) {
                onPunctuate(System.currentTimeMillis());
                try {
                    Thread.sleep(Math.min(25L, Math.max(1L, options.punctuateInterval().toMillis())));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StreamsException("Interrupted while waiting for async processor capacity", e);
                }
            }
            if (bufferedRecords() < options.maxBufferedRecords()) {
                return true;
            }
        }
        throw new StreamsException("Async processor buffer is full: " + options.maxBufferedRecords()
                + "; processor=" + options.processorName());
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
        if (closed) return;
        drainStoredOutputs();
        scanPendingStore(timestamp);
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

    private void scanPendingStore(long timestamp) {
        if (pendingStore == null) return;
        long now = System.currentTimeMillis();
        int scanned = 0;
        try (KeyValueIterator<String, AsyncPendingRecord<KIn, VIn>> iterator = pendingStore.all()) {
            while (iterator.hasNext() && scanned++ < options.recoveryScanMaxRecords()) {
                KeyValue<String, AsyncPendingRecord<KIn, VIn>> entry = iterator.next();
                AsyncPendingRecord<KIn, VIn> pending = entry.value;
                if (pending == null || inFlightIds.contains(pending.recordId())) continue;
                if (hasStoredOutput(pending.recordId())) continue;
                if (pending.notBeforeEpochMs() <= now) {
                    enqueueIfNotInFlight(new WorkItem<>(pending, Optional.empty()));
                }
            }
        }
        drainReady(now);
    }

    private boolean hasStoredOutput(String recordId) {
        if (outputStore == null) return false;
        String from = AsyncOutputRecord.prefix(recordId);
        String to = prefixUpperBound(from);
        try (KeyValueIterator<String, AsyncOutputRecord<KOut, VOut>> iterator = outputStore.range(from, to)) {
            return iterator.hasNext();
        }
    }

    private List<AsyncOutputRecord<KOut, VOut>> loadStoredOutputsBatch() {
        List<AsyncOutputRecord<KOut, VOut>> outputs = new ArrayList<>();
        if (outputStore == null) return outputs;
        try (KeyValueIterator<String, AsyncOutputRecord<KOut, VOut>> iterator = outputStore.all()) {
            while (iterator.hasNext() && outputs.size() < options.outputDrainBatchSize()) {
                AsyncOutputRecord<KOut, VOut> value = iterator.next().value;
                if (value != null) outputs.add(value);
            }
        }
        outputs.sort(Comparator
                .comparing(AsyncOutputRecord<KOut, VOut>::recordId)
                .thenComparingInt(AsyncOutputRecord::sequence));
        return outputs;
    }

    private void enqueueIfNotInFlight(WorkItem<KIn, VIn> item) {
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
        Completed<KIn, VIn, KOut, VOut> done;
        while ((done = completed.poll()) != null) {
            permits.release();
            inFlightIds.remove(done.item().pending().recordId());
            try {
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
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new StreamsException("Async processing completion failed", e);
            }
        }
    }

    private void handleSuccess(WorkItem<KIn, VIn> item, Collection<AsyncOutput<KOut, VOut>> outputs) {
        String recordId = item.pending().recordId();
        Record<KIn, VIn> original = item.pending().toRecord();

        if (outputStore != null) {
            Collection<AsyncOutput<KOut, VOut>> safeOutputs = outputs == null ? List.of() : outputs;
            if (safeOutputs.isEmpty()) {
                AsyncOutputRecord<KOut, VOut> terminal = AsyncOutputRecord.terminal(recordId);
                outputStore.put(terminal.storeKey(), terminal);
                counters.terminal.incrementAndGet();
            } else {
                int seq = 0;
                for (AsyncOutput<KOut, VOut> output : safeOutputs) {
                    AsyncOutputRecord<KOut, VOut> durable = AsyncOutputRecord.from(
                            recordId, seq++, output, original.timestamp(), original.headers());
                    outputStore.put(durable.storeKey(), durable);
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
        for (AsyncOutputRecord<KOut, VOut> out : loadStoredOutputsBatch()) {
            if (!out.terminalMarker()) {
                Record<KOut, VOut> record = out.toRecord();
                if (out.childName() == null || out.childName().isBlank()) {
                    context.forward(record);
                } else {
                    context.forward(record, out.childName());
                }
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

    private void forward(Collection<AsyncOutput<KOut, VOut>> outputs, Record<KIn, VIn> original, String recordId) {
        if (outputs == null) return;
        int seq = 0;
        for (AsyncOutput<KOut, VOut> output : outputs) {
            long timestamp = output.timestamp() == null ? original.timestamp() : output.timestamp();
            Record<KOut, VOut> out = new Record<>(output.key(), output.value(), timestamp, original.headers());
            if (output.childName() == null || output.childName().isBlank()) {
                context.forward(out);
            } else {
                context.forward(out, output.childName());
            }
            counters.forwarded.incrementAndGet();
            options.metricsListener().onRecordForwarded(options.processorName(), recordId, seq++);
        }
    }

    private void handleFailure(WorkItem<KIn, VIn> item, Throwable error) {
        AsyncPendingRecord<KIn, VIn> pending = item.pending();

        if (pending.attempt() < options.maxAttempts()) {
            int nextAttempt = pending.attempt() + 1;
            long delay = backoff(pending.attempt());
            long notBefore = System.currentTimeMillis() + delay;
            AsyncPendingRecord<KIn, VIn> retry = pending.retry(nextAttempt, notBefore, error);
            if (pendingStore != null) pendingStore.put(retry.recordId(), retry);
            counters.retried.incrementAndGet();
            options.metricsListener().onRecordRetried(options.processorName(), retry.recordId(), nextAttempt, notBefore);
            ready.offer(new WorkItem<>(retry, item.recordMetadata()));
            return;
        }

        Record<KIn, VIn> original = pending.toRecord();
        options.errorHandler().onFailure(original, error, pending.attempt());
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
        Queue<WorkItem<KIn, VIn>> queue = waitingByKey.get(key);
        WorkItem<KIn, VIn> next = queue == null ? null : queue.poll();
        if (next == null) {
            if (queue != null && queue.isEmpty()) waitingByKey.remove(key, queue);
        } else {
            ready.offer(next);
        }
    }

    private void drainReady(long now) {
        int iterations = ready.size();
        while (iterations-- > 0 && permits.tryAcquire()) {
            WorkItem<KIn, VIn> item = ready.poll();
            if (item == null) {
                permits.release();
                return;
            }
            if (item.pending().notBeforeEpochMs() > now) {
                ready.offer(item);
                permits.release();
                continue;
            }
            if (inFlightIds.contains(item.pending().recordId()) || hasStoredOutput(item.pending().recordId())) {
                permits.release();
                continue;
            }
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

    private void submit(WorkItem<KIn, VIn> item) {
        inFlightIds.add(item.pending().recordId());
        counters.submitted.incrementAndGet();
        options.metricsListener().onRecordSubmitted(options.processorName(), item.pending().recordId(), item.pending().attempt());
        try {
            options.executor().execute(() -> {
                try {
                    AsyncRecordContext asyncContext = new AsyncRecordContext(
                            item.pending().recordId(), item.pending().attempt(), item.recordMetadata());
                    CompletionStage<Collection<AsyncOutput<KOut, VOut>>> stage =
                            handler.process(item.pending().toRecord(), asyncContext);
                    stage.whenComplete((outputs, error) -> completed.offer(new Completed<>(item, outputs, error)));
                } catch (Throwable t) {
                    completed.offer(new Completed<>(item, null, t));
                }
            });
        } catch (Throwable t) {
            completed.offer(new Completed<>(item, null, t));
        }
    }

    private Object keyFor(AsyncPendingRecord<KIn, VIn> pending) {
        return pending.key() == null ? NullKey.INSTANCE : pending.key();
    }

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
            if (chars[i] != Character.MAX_VALUE) {
                chars[i]++;
                return new String(chars, 0, i + 1);
            }
        }
        return prefix + Character.MAX_VALUE;
    }

    @Override
    public void close() {
        closed = true;
        emitSnapshot();
    }

    private enum NullKey { INSTANCE }
    private record WorkItem<K, V>(AsyncPendingRecord<K, V> pending, Optional<RecordMetadata> recordMetadata) { }
    private record Completed<KIn, VIn, KOut, VOut>(
            WorkItem<KIn, VIn> item,
            Collection<AsyncOutput<KOut, VOut>> outputs,
            Throwable error
    ) { }
}
