package kafka.streams.async;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Kafka Streams ProcessorSupplier that executes user work asynchronously and forwards completed results
 * only from the Kafka Streams thread via punctuation.
 */
public final class AsyncProcessorSupplier<KIn, VIn, KOut, VOut>
        implements ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private final AsyncRecordHandler<KIn, VIn, KOut, VOut> handler;
    private final AsyncProcessorOptions<KIn, VIn, KOut, VOut> options;

    private AsyncProcessorSupplier(
            AsyncRecordHandler<KIn, VIn, KOut, VOut> handler,
            AsyncProcessorOptions<KIn, VIn, KOut, VOut> options
    ) {
        this.handler = Objects.requireNonNull(handler, "handler");
        this.options = Objects.requireNonNull(options, "options");
    }

    public static <KIn, VIn, KOut, VOut> AsyncProcessorSupplier<KIn, VIn, KOut, VOut> create(
            AsyncRecordHandler<KIn, VIn, KOut, VOut> handler,
            AsyncProcessorOptions<KIn, VIn, KOut, VOut> options
    ) {
        return new AsyncProcessorSupplier<>(handler, options);
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        return new AsyncProcessor<>(handler, options);
    }
}
