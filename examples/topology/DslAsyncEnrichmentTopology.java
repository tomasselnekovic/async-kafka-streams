package example;

import kafka.streams.async.AsyncOutput;
import kafka.streams.async.AsyncProcessorOptions;
import kafka.streams.async.AsyncProcessorSupplier;
import kafka.streams.async.AsyncRecordHandler;
import kafka.streams.async.AsyncStores;
import kafka.streams.async.BackpressureStrategy;
import kafka.streams.async.ErrorStrategy;
import kafka.streams.async.Ordering;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Example wiring the async handler API into the Kafka Streams DSL
 * ({@link StreamsBuilder}) instead of the low-level {@link Topology} API.
 */
public final class DslAsyncEnrichmentTopology {
    private static final String PENDING_STORE = "async-enrich-pending-store";
    private static final String OUTPUT_STORE = "async-enrich-output-store";

    public static Topology build(
            Executor asyncExecutor,
            WebClientLike webClient,
            Serde<InputEvent> inputEventSerde,
            Serde<OutputEvent> outputEventSerde
    ) {
        AsyncRecordHandler<String, InputEvent, String, OutputEvent> handler = (record, ctx) ->
                webClient.post("/enrich", ctx.recordId(), record.value())
                        .thenApply(out -> List.of(AsyncOutput.of(record.key(), out)));

        AsyncProcessorOptions<String, InputEvent, String, OutputEvent> options =
                AsyncProcessorOptions.<String, InputEvent, String, OutputEvent>builder(asyncExecutor)
                        .processorName("customer-enrichment")
                        .pendingStoreName(PENDING_STORE)
                        .outputStoreName(OUTPUT_STORE)
                        .ordering(Ordering.KEY)
                        .maxInFlight(500)
                        .maxBufferedRecords(10_000)
                        .maxAttempts(5)
                        .initialBackoff(Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofMinutes(5))
                        .backpressureStrategy(BackpressureStrategy.FAIL)
                        .errorStrategy(ErrorStrategy.FAIL_TASK)
                        .build();

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(AsyncStores.pendingStore(PENDING_STORE, Serdes.String(), inputEventSerde));
        builder.addStateStore(AsyncStores.outputStore(OUTPUT_STORE, Serdes.String(), outputEventSerde));

        KStream<String, OutputEvent> enriched = builder
                .stream("input-topic", Consumed.with(Serdes.String(), inputEventSerde))
                .process(
                        AsyncProcessorSupplier.create(handler, options),
                        Named.as("async-enrich"),
                        PENDING_STORE, OUTPUT_STORE
                );

        enriched.to("output-topic", Produced.with(Serdes.String(), outputEventSerde));
        return builder.build();
    }

    /** Minimal stand-in for an async HTTP client so this example has no external dependency. */
    public interface WebClientLike {
        java.util.concurrent.CompletableFuture<OutputEvent> post(String uri, String idempotencyKey, InputEvent body);
    }

    public record InputEvent(String id, String payload) { }
    public record OutputEvent(String id, String enrichedPayload) { }
}
