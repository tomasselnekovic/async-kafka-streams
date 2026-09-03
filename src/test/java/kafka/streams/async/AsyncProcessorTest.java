package kafka.streams.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.junit.jupiter.api.Test;

class AsyncProcessorTest {
    private static final Executor DIRECT = Runnable::run;

    @Test
    void forwardsAsyncResultFromPunctuator() {
        AsyncRecordHandler<String, String, String, String> handler =
                (record, ctx) -> CompletableFuture.completedFuture((java.util.Collection<AsyncOutput<String, String>>) List.of(AsyncOutput.of(record.key(), record.value().toUpperCase())));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology(handler, 3, ErrorStrategy.FAIL_TASK), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("a", "hello");
            driver.advanceWallClockTime(Duration.ofMillis(500));

            assertEquals("HELLO", output.readValue());
        }
    }

    @Test
    void retriesThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        AsyncRecordHandler<String, String, String, String> handler = (record, ctx) -> {
            if (attempts.incrementAndGet() == 1) {
                CompletableFuture<java.util.Collection<AsyncOutput<String, String>>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("first failure"));
                return failed;
            }
            return CompletableFuture.completedFuture((java.util.Collection<AsyncOutput<String, String>>) List.of(AsyncOutput.of(record.key(), "ok-" + ctx.attempt())));
        };

        try (TopologyTestDriver driver = new TopologyTestDriver(topology(handler, 3, ErrorStrategy.FAIL_TASK), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("a", "hello");
            driver.advanceWallClockTime(Duration.ofSeconds(2));

            assertEquals("ok-2", output.readValue());
            assertEquals(2, attempts.get());
        }
    }

    @Test
    void failsTaskAfterAttemptsExhausted() {
        AsyncRecordHandler<String, String, String, String> handler = (record, ctx) -> {
            CompletableFuture<java.util.Collection<AsyncOutput<String, String>>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("boom"));
            return failed;
        };

        try (TopologyTestDriver driver = new TopologyTestDriver(topology(handler, 1, ErrorStrategy.FAIL_TASK), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            input.pipeInput("a", "hello");
            assertThrows(StreamsException.class, () -> driver.advanceWallClockTime(Duration.ofMillis(500)));
        }
    }

    private static Topology topology(
            AsyncRecordHandler<String, String, String, String> handler,
            int maxAttempts,
            ErrorStrategy errorStrategy
    ) {
        String pending = "pending-store";
        String output = "output-store";
        AsyncProcessorOptions<String, String, String, String> options = AsyncProcessorOptions
                .<String, String, String, String>builder(DIRECT)
                .pendingStoreName(pending)
                .outputStoreName(output)
                .processorName("test-processor")
                .maxAttempts(maxAttempts)
                .initialBackoff(Duration.ZERO)
                .maxBackoff(Duration.ZERO)
                .punctuateInterval(Duration.ofMillis(10))
                .recoveryScanInterval(Duration.ofMillis(50))
                .storeCommitBarrierDelay(Duration.ZERO)
                .errorStrategy(errorStrategy)
                .build();

        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), "input");
        topology.addProcessor("async", AsyncProcessorSupplier.create(handler, options), "source");
        topology.addStateStore(AsyncStores.pendingStore(pending, Serdes.String(), Serdes.String()), "async");
        topology.addStateStore(AsyncStores.outputStore(output, Serdes.String(), Serdes.String()), "async");
        topology.addSink("sink", "output", Serdes.String().serializer(), Serdes.String().serializer(), "async");
        return topology;
    }

    private static Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "async-processor-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-test-" + System.nanoTime());
        return props;
    }
}
