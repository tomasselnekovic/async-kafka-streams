package kafka.streams.async.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import kafka.streams.async.AsyncOutput;
import kafka.streams.async.AsyncProcessorOptions;
import kafka.streams.async.AsyncProcessorSupplier;
import kafka.streams.async.AsyncStores;
import kafka.streams.async.BackpressureStrategy;
import kafka.streams.async.CorrectnessMode;
import kafka.streams.async.ErrorStrategy;
import kafka.streams.async.Ordering;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for error handling, retry, and backpressure behaviours.
 *
 * <ul>
 *   <li><b>Retry on transient failure</b>: handler fails on the first attempt and succeeds
 *       on the second; the retry must be transparent and the final output must appear.</li>
 *   <li><b>SKIP after exhausted retries</b>: when all attempts fail and {@link ErrorStrategy#SKIP}
 *       is configured, no output record appears but the {@code AsyncErrorHandler} is invoked.</li>
 *   <li><b>BLOCK backpressure</b>: when the buffer is full, the processor blocks (instead of
 *       failing) until capacity is available, allowing slow handlers to process all records
 *       without data loss.</li>
 * </ul>
 */
class ErrorHandlingIT extends IntegrationTestBase {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingIT.class);

    // ------------------------------------------------------------------
    // 1. Handler fails once, retries and succeeds
    // ------------------------------------------------------------------

    /**
     * The handler throws on the first attempt and succeeds on subsequent ones.
     * {@code maxAttempts=3} is configured; the output topic must receive exactly one
     * record containing the attempt number that succeeded.
     */
    @Test
    void retriesOnTransientFailureAndEventuallySucceeds() throws Exception {
        String inputTopic  = uniqueTopic("retry-in");
        String outputTopic = uniqueTopic("retry-out");
        createTopics(inputTopic, outputTopic);

        AtomicInteger attemptCounter = new AtomicInteger(0);

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        String pending = "retry-pending-" + inputTopic;
        String output  = "retry-output-"  + inputTopic;

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("retry-proc")
                        .ordering(Ordering.KEY)
                        .maxInFlight(5)
                        .maxAttempts(3)
                        .initialBackoff(Duration.ofMillis(100))
                        .maxBackoff(Duration.ofMillis(200))
                        .punctuateInterval(Duration.ofMillis(50))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .errorStrategy(ErrorStrategy.FAIL_TASK)
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> {
                    int attempt = attemptCounter.incrementAndGet();
                    if (attempt == 1) {
                        CompletableFuture<java.util.Collection<AsyncOutput<String, String>>> failed =
                                new CompletableFuture<>();
                        failed.completeExceptionally(new RuntimeException("transient failure on attempt 1"));
                        return failed;
                    }
                    return CompletableFuture.completedFuture(
                            List.of(AsyncOutput.of(record.key(), "success-on-attempt-" + ctx.attempt())));
                };

        Topology topology = buildTopology(inputTopic, outputTopic, pending, output, options, handler);
        String appId    = "retry-it-" + inputTopic;
        String stateDir = uniqueStateDir();

        try (KafkaStreams ignored = startStreams(topology, appId, stateDir)) {
            produceSingle(inputTopic, "k1", "hello");
            List<ConsumerRecord<String, String>> records = consume(outputTopic, 1, Duration.ofSeconds(30));

            assertEquals(1, records.size(), "Exactly one output record expected");
            assertEquals("success-on-attempt-2", records.getFirst().value(),
                    "Output must reflect the second attempt");
            log.info("[retriesOnTransientFailureAndEventuallySucceeds] PASS – output='{}', totalAttempts={}",
                    records.getFirst().value(), attemptCounter.get());
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 2. SKIP drops failed record and calls the error handler
    // ------------------------------------------------------------------

    /**
     * All attempts fail. {@link ErrorStrategy#SKIP} is configured, so the record is dropped
     * (no output), but the registered {@code AsyncErrorHandler} is called with the original
     * record and the error.
     */
    @Test
    void skipStrategyDropsFailedRecordAndInvokesErrorHandler() throws Exception {
        String inputTopic  = uniqueTopic("skip-in");
        String outputTopic = uniqueTopic("skip-out");
        createTopics(inputTopic, outputTopic);

        CopyOnWriteArrayList<String> errorHandlerValues = new CopyOnWriteArrayList<>();

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        String pending = "skip-pending-" + inputTopic;
        String output  = "skip-output-"  + inputTopic;

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("skip-proc")
                        .ordering(Ordering.KEY)
                        .maxInFlight(5)
                        .maxAttempts(2)
                        .initialBackoff(Duration.ofMillis(50))
                        .maxBackoff(Duration.ofMillis(100))
                        .punctuateInterval(Duration.ofMillis(50))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .errorStrategy(ErrorStrategy.SKIP)
                        .errorHandler((record, error, attempts) -> {
                            errorHandlerValues.add(record.value());
                            log.info("[skip] errorHandler called: key={}, value={}, attempts={}",
                                    record.key(), record.value(), attempts);
                        })
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> {
                    CompletableFuture<java.util.Collection<AsyncOutput<String, String>>> failed =
                            new CompletableFuture<>();
                    failed.completeExceptionally(new RuntimeException("always fails"));
                    return failed;
                };

        Topology topology = buildTopology(inputTopic, outputTopic, pending, output, options, handler);
        String appId    = "skip-it-" + inputTopic;
        String stateDir = uniqueStateDir();

        try (KafkaStreams ignored = startStreams(topology, appId, stateDir)) {
            produceSingle(inputTopic, "k1", "drop-me");

            // Wait long enough for retries + skip decision; verify no output record arrives
            Thread.sleep(3_000);
            List<ConsumerRecord<String, String>> outputRecords = consume(outputTopic, 1, Duration.ofSeconds(5));

            assertTrue(outputRecords.isEmpty(),
                    "SKIP strategy must produce no output records, but got: " + outputRecords.size());
            assertEquals(1, errorHandlerValues.size(),
                    "Error handler must be called exactly once");
            assertEquals("drop-me", errorHandlerValues.getFirst(),
                    "Error handler must receive the original record value");
            log.info("[skipStrategyDropsFailedRecordAndInvokesErrorHandler] PASS");
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // 3. BLOCK backpressure lets slow handlers finish without data loss
    // ------------------------------------------------------------------

    /**
     * A handler that sleeps 80 ms per record is paired with a tiny buffer
     * ({@code maxBufferedRecords=5}) and {@link BackpressureStrategy#BLOCK}. A burst
     * of 20 records is produced. The processor must block the stream-thread when the
     * buffer is full and eventually forward all 20 records – no records must be lost
     * or cause a {@code StreamsException}.
     */
    @Test
    void backpressureBlockAllowsSlowHandlerToProcessAllRecords() throws Exception {
        String inputTopic  = uniqueTopic("backpressure-in");
        String outputTopic = uniqueTopic("backpressure-out");
        createTopics(inputTopic, outputTopic);

        int totalRecords    = 20;
        int maxBuffered     = 6;
        long handlerSleepMs = 80;

        ExecutorService exec = Executors.newFixedThreadPool(maxBuffered);
        String pending = "backpressure-pending-" + inputTopic;
        String output  = "backpressure-output-"  + inputTopic;

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("backpressure-proc")
                        .ordering(Ordering.UNORDERED)
                        .maxInFlight(maxBuffered)
                        .maxBufferedRecords(maxBuffered)
                        .maxAttempts(1)
                        .punctuateInterval(Duration.ofMillis(30))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .errorStrategy(ErrorStrategy.FAIL_TASK)
                        .backpressureStrategy(BackpressureStrategy.BLOCK)
                        .backpressureBlockTimeout(Duration.ofSeconds(30))
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> CompletableFuture.supplyAsync(() -> {
                    try { Thread.sleep(handlerSleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return (java.util.Collection<AsyncOutput<String, String>>)
                            List.of(AsyncOutput.of(record.key(), record.value() + "-ok"));
                }, exec);

        Topology topology = buildTopology(inputTopic, outputTopic, pending, output, options, handler);
        String appId    = "backpressure-it-" + inputTopic;
        String stateDir = uniqueStateDir();

        try (KafkaStreams ignored = startStreams(topology, appId, stateDir)) {
            // Flood the topic – more records than maxBufferedRecords
            produceWithDistinctKeys(inputTopic, totalRecords, totalRecords);
            List<ConsumerRecord<String, String>> records = consume(outputTopic, totalRecords, Duration.ofMinutes(2));

            assertEquals(totalRecords, records.size(),
                    "All " + totalRecords + " records must arrive despite buffer pressure");
            assertTrue(records.stream().allMatch(r -> r.value().endsWith("-ok")),
                    "All records must be processed successfully");
            log.info("[backpressureBlockAllowsSlowHandlerToProcessAllRecords] PASS – {} records", totalRecords);
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Topology builder
    // ------------------------------------------------------------------

    private static Topology buildTopology(
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
                        Named.as("async-proc"),
                        pendingStore, outputStore)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }
}

