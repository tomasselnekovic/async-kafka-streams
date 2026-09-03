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
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class AsyncEnrichmentTopology {
    @Bean
    Topology topology(
            Executor asyncStreamsExecutor,
            WebClient webClient,
            Serde<InputEvent> inputEventSerde,
            Serde<OutputEvent> outputEventSerde
    ) {
        String pendingStore = "async-enrichment-pending";
        String outputStore = "async-enrichment-output";

        AsyncRecordHandler<String, InputEvent, String, OutputEvent> handler = (record, ctx) ->
                webClient.post()
                        .uri("/enrich")
                        .header("Idempotency-Key", ctx.recordId())
                        .bodyValue(record.value())
                        .retrieve()
                        .bodyToMono(OutputEvent.class)
                        .map(out -> List.of(AsyncOutput.of(record.key(), out)))
                        .toFuture();

        AsyncProcessorOptions<String, InputEvent, String, OutputEvent> options =
                AsyncProcessorOptions.<String, InputEvent, String, OutputEvent>builder(asyncStreamsExecutor)
                        .processorName("async-enrichment")
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

        Topology topology = new Topology();
        topology.addSource("source", Serdes.String().deserializer(), inputEventSerde.deserializer(), "input-topic");
        topology.addProcessor("async-enrich", AsyncProcessorSupplier.create(handler, options), "source");
        topology.addStateStore(AsyncStores.pendingStore(pendingStore, Serdes.String(), inputEventSerde), "async-enrich");
        topology.addStateStore(AsyncStores.outputStore(outputStore, Serdes.String(), outputEventSerde), "async-enrich");
        topology.addSink("sink", "output-topic", Serdes.String().serializer(), outputEventSerde.serializer(), "async-enrich");
        return topology;
    }

    public record InputEvent(String id, String payload) { }
    public record OutputEvent(String id, String enrichedPayload) { }
}
