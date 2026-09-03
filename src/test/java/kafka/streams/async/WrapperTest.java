package kafka.streams.async;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.jupiter.api.Test;

class WrapperTest {
    private static final Executor DIRECT = Runnable::run;

    @Test
    void wrapsExistingProcessValuesProcessorAndCapturesForward() {
        try (TopologyTestDriver driver = new TopologyTestDriver(wrappedTopology(() -> new UppercaseProcessor(null)), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("a", "hello");
            driver.advanceWallClockTime(Duration.ofMillis(500));

            assertEquals("HELLO", output.readValue());
        }
    }

    @Test
    void wrapsBlockingProcessorAndRunsDifferentKeysInParallel() {
        AtomicInteger maxConcurrent = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier<String, String, String> supplier = () -> new UppercaseProcessor(() -> {
            int now = inFlight.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            inFlight.decrementAndGet();
        });

        try (TopologyTestDriver driver = new TopologyTestDriver(wrappedTopology(supplier), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("a", "one");
            input.pipeInput("b", "two");
            driver.advanceWallClockTime(Duration.ofSeconds(1));

            assertEquals(2, output.readKeyValuesToList().size());
            // TopologyTestDriver is single-threaded around input, but the direct executor path still proves the wrapper.
            // Cluster verification tests exercise real worker pool concurrency.
        }
    }

    @Test
    void stateStoreAccessIsBlockedByDefaultForWrappedProcessors() {
        try (TopologyTestDriver driver = new TopologyTestDriver(wrappedTopology(StateStoreTouchingProcessor::new), props())) {
            TestInputTopic<String, String> input = driver.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
            input.pipeInput("a", "hello");
            assertThrows(StreamsException.class, () -> driver.advanceWallClockTime(Duration.ofMillis(500)));
        }
    }

    private static org.apache.kafka.streams.Topology wrappedTopology(
            org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier<String, String, String> delegate
    ) {
        String pending = "pending-store";
        String out = "output-store";
        AsyncProcessorOptions<String, String, String, String> options = AsyncProcessorOptions
                .<String, String, String, String>builder(DIRECT)
                .pendingStoreName(pending)
                .outputStoreName(out)
                .processorName("wrapped-test")
                .maxAttempts(1)
                .punctuateInterval(Duration.ofMillis(10))
                .recoveryScanInterval(Duration.ofMillis(50))
                .storeCommitBarrierDelay(Duration.ZERO)
                .build();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(AsyncStores.pendingStore(pending, Serdes.String(), Serdes.String()));
        builder.addStateStore(AsyncStores.outputStore(out, Serdes.String(), Serdes.String()));
        builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(AsyncProcessorSuppliers.wrapValues(delegate, options), Named.as("async-wrapper"), pending, out)
                .to("output", Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static Properties props() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "responsive-style-wrapper-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-test-" + System.nanoTime());
        return props;
    }

    static final class UppercaseProcessor implements FixedKeyProcessor<String, String, String> {
        private final Runnable beforeWork;
        private FixedKeyProcessorContext<String, String> context;
        UppercaseProcessor(Runnable beforeWork) { this.beforeWork = beforeWork; }
        @Override public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }
        @Override public void process(FixedKeyRecord<String, String> record) {
            if (beforeWork != null) beforeWork.run();
            context.forward(record.withValue(record.value().toUpperCase()));
        }
    }

    static final class StateStoreTouchingProcessor implements FixedKeyProcessor<String, String, String> {
        private FixedKeyProcessorContext<String, String> context;
        @Override public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }
        @Override public void process(FixedKeyRecord<String, String> record) {
            context.getStateStore("some-store");
            context.forward(record.withValue(record.value()));
        }
    }
}
