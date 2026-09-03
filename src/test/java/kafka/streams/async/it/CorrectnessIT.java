package kafka.streams.async.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import kafka.streams.async.AsyncOutput;
import kafka.streams.async.AsyncProcessorOptions;
import kafka.streams.async.AsyncProcessorSupplier;
import kafka.streams.async.AsyncProcessorSuppliers;
import kafka.streams.async.AsyncStores;
import kafka.streams.async.CorrectnessMode;
import kafka.streams.async.Ordering;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests that verify the <em>correctness</em> properties of the async processor:
 * <ul>
 *   <li>KEY ordering is preserved end-to-end even when async handlers sleep.</li>
 *   <li>UNORDERED mode lets different keys run in parallel (concurrency is real).</li>
 *   <li>Crash-recovery: records persisted in the pending store are re-processed after restart.</li>
 * </ul>
 */
class CorrectnessIT extends IntegrationTestBase {

    private static final Logger log = LoggerFactory.getLogger(CorrectnessIT.class);

    // ------------------------------------------------------------------
    // 1. KEY ordering preserved despite async delays
    // ------------------------------------------------------------------

    /**
     * Produces 20 sequentially numbered values for the same key through a handler
     * that sleeps a random short time per record. With {@link Ordering#KEY} the
     * output sequence must exactly match the input sequence.
     */
    @Test
    void keyOrderingIsPreservedEndToEnd() throws Exception {
        String inputTopic  = uniqueTopic("key-order-in");
        String outputTopic = uniqueTopic("key-order-out");
        createTopics(inputTopic, outputTopic);

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        String pending = "key-order-pending-" + inputTopic;
        String output  = "key-order-output-"  + inputTopic;

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("key-order-proc")
                        .ordering(Ordering.KEY)
                        .maxInFlight(20)
                        .maxAttempts(1)
                        .punctuateInterval(Duration.ofMillis(50))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .build();

        Topology topology = buildProcessValuesTopology(
                inputTopic, outputTopic, pending, output,
                () -> new SleepThenPassthroughProcessor(10), options);

        String stateDir = uniqueStateDir();
        String appId    = "key-order-it-" + inputTopic;

        int count = 20;
        List<String> sentValues = IntStream.range(0, count).mapToObj(i -> "msg-" + i).toList();

        try (KafkaStreams ignored = startStreams(topology, appId, stateDir)) {
            produceValues(inputTopic, "singleKey", sentValues);
            List<ConsumerRecord<String, String>> records = consume(outputTopic, count, Duration.ofSeconds(30));

            assertEquals(count, records.size(), "All " + count + " records must be forwarded");

            // Output order must match input order for KEY mode
            List<String> receivedValues = records.stream().map(ConsumerRecord::value).toList();
            assertEquals(sentValues, receivedValues,
                    "KEY ordering must be preserved end-to-end; got: " + receivedValues);
            log.info("[keyOrderingIsPreservedEndToEnd] PASS – {} records in correct order", count);
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 2. UNORDERED mode achieves real concurrency
    // ------------------------------------------------------------------

    /**
     * Sends 10 records with 10 distinct keys through a handler that sleeps 200 ms each.
     * With {@link Ordering#UNORDERED} and {@code maxInFlight=10} they all run in parallel,
     * so total wall-clock time must be substantially less than 10 × 200 ms = 2 000 ms.
     */
    @Test
    void unorderedModeRunsDistinctKeysConcurrently() throws Exception {
        String inputTopic  = uniqueTopic("unordered-in");
        String outputTopic = uniqueTopic("unordered-out");
        createTopics(inputTopic, outputTopic);

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        String pending = "unordered-pending-" + inputTopic;
        String output  = "unordered-output-"  + inputTopic;

        int numKeys         = 10;
        long handlerSleepMs = 200;
        long serialTimeMs   = numKeys * handlerSleepMs;

        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger inFlight      = new AtomicInteger(0);

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("unordered-proc")
                        .ordering(Ordering.UNORDERED)
                        .maxInFlight(numKeys)
                        .maxAttempts(1)
                        .punctuateInterval(Duration.ofMillis(50))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> CompletableFuture.supplyAsync(() -> {
                    int now = inFlight.incrementAndGet();
                    maxConcurrent.accumulateAndGet(now, Math::max);
                    try { Thread.sleep(handlerSleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    inFlight.decrementAndGet();
                    return (java.util.Collection<AsyncOutput<String, String>>)
                            List.of(AsyncOutput.of(record.key(), record.value() + "-done"));
                }, exec);

        Topology topology = buildHandlerTopology(inputTopic, outputTopic, pending, output, options, handler);
        String stateDir = uniqueStateDir();
        String appId    = "unordered-it-" + inputTopic;

        try (KafkaStreams ignored = startStreams(topology, appId, stateDir)) {
            // Start timer AFTER streams is running to exclude startup latency from the measurement.
            // Batch-produce all records with one producer to avoid per-call TCP/metadata overhead.
            String[] kvPairs = new String[numKeys * 2];
            for (int i = 0; i < numKeys; i++) { kvPairs[i * 2] = "key-" + i; kvPairs[i * 2 + 1] = "v" + i; }
            long wallStart = System.currentTimeMillis();
            produce(inputTopic, kvPairs);
            List<ConsumerRecord<String, String>> records = consume(outputTopic, numKeys, Duration.ofSeconds(30));
            long wallElapsed = System.currentTimeMillis() - wallStart;

            assertEquals(numKeys, records.size(), "All records must be forwarded");

            log.info("[unorderedModeRunsDistinctKeysConcurrently] wall={} ms, maxConcurrent={}, serialWouldBe={} ms",
                    wallElapsed, maxConcurrent.get(), serialTimeMs);

            // Must be at least 2x faster than serial (with 10 truly parallel keys it should be ~10x)
            assertTrue(wallElapsed < serialTimeMs / 2,
                    "Expected async wall time " + wallElapsed + " ms to be < " + (serialTimeMs / 2) + " ms (serial=" + serialTimeMs + " ms)");
            assertTrue(maxConcurrent.get() >= 2,
                    "Expected at least 2 concurrent handler executions, got " + maxConcurrent.get());
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 3. Crash recovery: pending store survives restart
    // ------------------------------------------------------------------

    /**
     * Simulates a crash by:
     * <ol>
     *   <li>Starting a Streams instance that stores records in the pending store but uses
     *       a very long {@code recoveryScanInterval} so they are never submitted before
     *       the instance is force-closed.</li>
     *   <li>Starting a second Streams instance on the same app-ID / state-dir with a short
     *       {@code recoveryScanInterval}, which finds the pending records and completes them.</li>
     * </ol>
     * This verifies that the durable pending store is the crash-safety guarantee described
     * in {@code CRASH_SAFETY.md}.
     */
    @Test
    void pendingStoreRecordsAreRecoveredAfterRestart() throws Exception {
        String inputTopic  = uniqueTopic("crash-recovery-in");
        String outputTopic = uniqueTopic("crash-recovery-out");
        createTopics(inputTopic, outputTopic);

        String pending  = "crash-recovery-pending-" + inputTopic;
        String output   = "crash-recovery-output-"  + inputTopic;
        String appId    = "crash-recovery-it-" + inputTopic;
        String stateDir = uniqueStateDir();

        int recordCount = 5;
        ExecutorService exec1 = Executors.newVirtualThreadPerTaskExecutor();

        // --- FIRST INSTANCE: stores records but never submits (recoveryScanInterval = 10 min) ---
        AsyncProcessorOptions<String, String, String, String> optionsFirstRun =
                AsyncProcessorOptions.<String, String, String, String>builder(exec1)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("crash-recovery-proc")
                        .ordering(Ordering.KEY)
                        .maxInFlight(10)
                        .maxAttempts(3)
                        .punctuateInterval(Duration.ofMillis(100))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .recoveryScanInterval(Duration.ofMinutes(10))   // effectively never scans
                        .correctnessMode(CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT)
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> identity =
                (record, ctx) -> CompletableFuture.completedFuture(
                        List.of(AsyncOutput.of(record.key(), record.value() + "-recovered")));

        Topology topologyFirstRun = buildHandlerTopology(
                inputTopic, outputTopic, pending, output, optionsFirstRun, identity);

        log.info("[pendingStoreRecordsAreRecoveredAfterRestart] Starting first instance (stores but never submits async work)");
        KafkaStreams firstInstance = startStreams(topologyFirstRun, appId, stateDir);
        try {
            // Produce records – they land in the pending store but the scan interval is huge
            for (int i = 0; i < recordCount; i++) {
                produceSingle(inputTopic, "key-" + i, "value-" + i);
            }
            // Give Streams time to consume and write to the pending store
            Thread.sleep(2_000);
        } finally {
            firstInstance.close(Duration.ofSeconds(5));
            exec1.shutdownNow();
        }

        // --- SECOND INSTANCE: short recoveryScanInterval, picks up from pending store ---
        ExecutorService exec2 = Executors.newVirtualThreadPerTaskExecutor();
        AsyncProcessorOptions<String, String, String, String> optionsSecondRun =
                AsyncProcessorOptions.<String, String, String, String>builder(exec2)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("crash-recovery-proc")
                        .ordering(Ordering.KEY)
                        .maxInFlight(10)
                        .maxAttempts(3)
                        .punctuateInterval(Duration.ofMillis(100))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .recoveryScanInterval(Duration.ofSeconds(2))    // quick scan
                        .correctnessMode(CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT)
                        .build();

        Topology topologySecondRun = buildHandlerTopology(
                inputTopic, outputTopic, pending, output, optionsSecondRun, identity);

        log.info("[pendingStoreRecordsAreRecoveredAfterRestart] Starting second instance (recovery)");
        try (KafkaStreams ignored = startStreams(topologySecondRun, appId, stateDir)) {
            // Wait for recovery scan to kick in and process all pending records
            List<ConsumerRecord<String, String>> records = consume(outputTopic, recordCount, Duration.ofSeconds(60));

            assertEquals(recordCount, records.size(),
                    "All " + recordCount + " pending records must be recovered and forwarded after restart");
            log.info("[pendingStoreRecordsAreRecoveredAfterRestart] PASS – {} records recovered", recordCount);
        } finally {
            exec2.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Topology builders
    // ------------------------------------------------------------------

    private static Topology buildProcessValuesTopology(
            String inputTopic, String outputTopic,
            String pendingStore, String outputStore,
            java.util.function.Supplier<FixedKeyProcessor<String, String, String>> procSupplier,
            AsyncProcessorOptions<String, String, String, String> options) {

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(AsyncStores.pendingStore(pendingStore, Serdes.String(), Serdes.String()));
        builder.addStateStore(AsyncStores.outputStore(outputStore, Serdes.String(), Serdes.String()));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(
                        AsyncProcessorSuppliers.wrapValues(procSupplier::get, options),
                        Named.as("async-wrap"),
                        pendingStore, outputStore)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Topology buildHandlerTopology(
            String inputTopic, String outputTopic,
            String pendingStore, String outputStore,
            AsyncProcessorOptions<String, String, String, String> options,
            kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler) {

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(AsyncStores.pendingStore(pendingStore, Serdes.String(), Serdes.String()));
        builder.addStateStore(AsyncStores.outputStore(outputStore, Serdes.String(), Serdes.String()));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        AsyncProcessorSupplier.create(handler, options),
                        Named.as("async-handler"),
                        pendingStore, outputStore)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    // ------------------------------------------------------------------
    // Inner helpers
    // ------------------------------------------------------------------

    /** A FixedKeyProcessor that sleeps {@code ms} milliseconds then forwards the record unchanged. */
    static final class SleepThenPassthroughProcessor
            implements FixedKeyProcessor<String, String, String> {

        private final long sleepMs;
        private FixedKeyProcessorContext<String, String> context;

        SleepThenPassthroughProcessor(long sleepMs) { this.sleepMs = sleepMs; }

        @Override
        public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            try { Thread.sleep(sleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            context.forward(record);
        }
    }
}

