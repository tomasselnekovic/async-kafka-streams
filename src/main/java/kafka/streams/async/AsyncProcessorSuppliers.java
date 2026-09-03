package kafka.streams.async;

import java.util.Objects;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Responsive-style factory methods for wrapping existing Kafka Streams processors.
 *
 * <p>The most useful method is {@link #wrapValues(FixedKeyProcessorSupplier, AsyncProcessorOptions)}:
 * it lets a normal processValues processor run on the async worker pool while all actual
 * Kafka Streams forwarding is replayed later from the stream thread.</p>
 */
public final class AsyncProcessorSuppliers {
    private AsyncProcessorSuppliers() { }

    /** Existing handler-style API. */
    public static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> fromHandler(
            AsyncRecordHandler<KIn, VIn, KOut, VOut> handler,
            AsyncProcessorOptions<KIn, VIn, KOut, VOut> options
    ) {
        return AsyncProcessorSupplier.create(handler, options);
    }

    /**
     * Wrap an existing FixedKeyProcessorSupplier for use with KStream.processValues(...).
     *
     * <p>This gives the desired Responsive-style usage:</p>
     *
     * <pre>{@code
     * stream.processValues(
     *     AsyncProcessorSuppliers.wrapValues(new MyProcessorSupplier(), options),
     *     Named.as("AsyncMyProcessor"),
     *     MY_STORE, ASYNC_PENDING_STORE, ASYNC_OUTPUT_STORE
     * ).to("output");
     * }</pre>
     *
     * <p>The delegate processor is executed on the async executor. It receives a buffered
     * context: calls to forward(...) are captured, persisted, and later forwarded from the
     * Kafka Streams thread.</p>
     */
    public static <K, VIn, VOut> FixedKeyProcessorSupplier<K, VIn, VOut> wrapValues(
            FixedKeyProcessorSupplier<K, VIn, VOut> delegate,
            AsyncProcessorOptions<K, VIn, K, VOut> options
    ) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(options, "options");
        return () -> new AsyncFixedKeyProcessor<>(delegate, options);
    }
}
