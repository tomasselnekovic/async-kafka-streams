package kafka.streams.async.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import kafka.streams.async.AsyncOutput;
import kafka.streams.async.AsyncProcessorOptions;
import kafka.streams.async.AsyncProcessorSupplier;
import kafka.streams.async.AsyncStores;
import kafka.streams.async.CorrectnessMode;
import kafka.streams.async.Ordering;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
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
 * Throughput benchmark comparing synchronous inline processing against async parallel processing.
 *
 * <p>Both topologies simulate an I/O-bound operation (Thread.sleep). The synchronous topology
 * blocks the Kafka Streams stream-thread; the async topology offloads the sleep to a virtual-thread
 * executor, allowing many operations to run in parallel.
 *
 * <p>The async variant must complete in a fraction of the time the synchronous variant takes,
 * proving the library's core value proposition.
 */
class ThroughputIT extends IntegrationTestBase {

    private static final Logger log = LoggerFactory.getLogger(ThroughputIT.class);

    private static final int    RECORD_COUNT  = 200;
    private static final long   IO_SLEEP_MS   = 20;     // simulated I/O latency per record
    private static final int    MAX_IN_FLIGHT = 50;
    private static final double MIN_SPEEDUP   = 4.0;    // async must be at least 4× faster

    /**
     * Runs the synchronous baseline and the async topology back-to-back and asserts
     * that the async topology is at least {@value #MIN_SPEEDUP}× faster.
     */
    @Test
    void asyncThroughputSignificantlyExceedsSynchronous() throws Exception {

        long syncMs  = measureSynchronousTopology();
        long asyncMs = measureAsyncTopology();

        double speedup = (double) syncMs / asyncMs;
        log.info("=== THROUGHPUT BENCHMARK ({} records, {} ms I/O each) ===", RECORD_COUNT, IO_SLEEP_MS);
        log.info("  Synchronous           : {} ms  ({} records/s)",
                syncMs, recordsPerSecond(syncMs));
        log.info("  Async ({} in-flight)  : {} ms  ({} records/s)",
                MAX_IN_FLIGHT, asyncMs, recordsPerSecond(asyncMs));
        log.info("  Speedup               : {}x", String.format("%.1f", speedup));

        assertTrue(speedup >= MIN_SPEEDUP,
                String.format("Expected async speedup >= %.1f× but got %.1f× (sync=%d ms, async=%d ms)",
                        MIN_SPEEDUP, speedup, syncMs, asyncMs));
    }

    // ------------------------------------------------------------------
    // Synchronous baseline
    // ------------------------------------------------------------------

    private long measureSynchronousTopology() throws Exception {
        String inputTopic  = uniqueTopic("throughput-sync-in");
        String outputTopic = uniqueTopic("throughput-sync-out");
        createTopics(inputTopic, outputTopic);

        // Plain processValues topology that sleeps inline on the StreamThread
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(
                        () -> new SynchronousSleeperProcessor(IO_SLEEP_MS),
                        Named.as("sync-sleeper"))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        String appId    = "throughput-sync-" + inputTopic;
        String stateDir = uniqueStateDir();

        try (KafkaStreams ignored = startStreams(builder.build(), appId, stateDir)) {
            long start = System.currentTimeMillis();
            produceWithDistinctKeys(inputTopic, RECORD_COUNT, RECORD_COUNT);
            List<ConsumerRecord<String, String>> records =
                    consume(outputTopic, RECORD_COUNT, Duration.ofMinutes(5));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals(RECORD_COUNT, records.size(), "Sync topology must forward all records");
            log.info("[sync] {} records in {} ms", RECORD_COUNT, elapsed);
            return elapsed;
        }
    }

    // ------------------------------------------------------------------
    // Async topology
    // ------------------------------------------------------------------

    private long measureAsyncTopology() throws Exception {
        String inputTopic  = uniqueTopic("throughput-async-in");
        String outputTopic = uniqueTopic("throughput-async-out");
        createTopics(inputTopic, outputTopic);

        ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor();
        String pending = "throughput-async-pending-" + inputTopic;
        String output  = "throughput-async-output-"  + inputTopic;

        AsyncProcessorOptions<String, String, String, String> options =
                AsyncProcessorOptions.<String, String, String, String>builder(exec)
                        .pendingStoreName(pending)
                        .outputStoreName(output)
                        .processorName("throughput-async-proc")
                        .ordering(Ordering.UNORDERED)
                        .maxInFlight(MAX_IN_FLIGHT)
                        .maxBufferedRecords(RECORD_COUNT + 100)
                        .maxAttempts(1)
                        .punctuateInterval(Duration.ofMillis(20))
                        .storeCommitBarrierDelay(Duration.ZERO)
                        .correctnessMode(CorrectnessMode.FAST_IN_MEMORY_SUBMIT)
                        .build();

        kafka.streams.async.AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> CompletableFuture.supplyAsync(() -> {
                    try { Thread.sleep(IO_SLEEP_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    return (java.util.Collection<AsyncOutput<String, String>>)
                            List.of(AsyncOutput.of(record.key(), record.value()));
                }, exec);

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(AsyncStores.pendingStore(pending, Serdes.String(), Serdes.String()));
        builder.addStateStore(AsyncStores.outputStore(output, Serdes.String(), Serdes.String()));
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        AsyncProcessorSupplier.create(handler, options),
                        Named.as("async-sleeper"),
                        pending, output)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        String appId    = "throughput-async-" + inputTopic;
        String stateDir = uniqueStateDir();

        try (KafkaStreams ignored = startStreams(builder.build(), appId, stateDir)) {
            long start = System.currentTimeMillis();
            produceWithDistinctKeys(inputTopic, RECORD_COUNT, RECORD_COUNT);
            List<ConsumerRecord<String, String>> records =
                    consume(outputTopic, RECORD_COUNT, Duration.ofMinutes(3));
            long elapsed = System.currentTimeMillis() - start;
            assertEquals(RECORD_COUNT, records.size(), "Async topology must forward all records");
            log.info("[async] {} records in {} ms", RECORD_COUNT, elapsed);
            return elapsed;
        } finally {
            exec.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    // Inner helpers
    // ------------------------------------------------------------------

    private static long recordsPerSecond(long ms) {
        return ms == 0 ? Long.MAX_VALUE : (RECORD_COUNT * 1000L / ms);
    }

    /** A FixedKeyProcessor that sleeps {@code ms} milliseconds on the Streams stream-thread. */
    static final class SynchronousSleeperProcessor
            implements FixedKeyProcessor<String, String, String> {

        private final long sleepMs;
        private FixedKeyProcessorContext<String, String> context;

        SynchronousSleeperProcessor(long sleepMs) { this.sleepMs = sleepMs; }

        @Override
        public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            try { Thread.sleep(sleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            context.forward(record);
        }
    }
}

