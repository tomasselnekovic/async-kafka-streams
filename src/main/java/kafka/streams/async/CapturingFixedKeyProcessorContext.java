package kafka.streams.async;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.StateStore;

/**
 * Context passed to a wrapped processor while it runs on the async worker thread.
 * It captures forward(...) calls and exposes safe read-only metadata methods.
 */
final class CapturingFixedKeyProcessorContext<K, VOut> implements FixedKeyProcessorContext<K, VOut> {
    private final FixedKeyProcessorContext<K, VOut> delegate;
    private final String recordId;
    private final StateStoreAccessPolicy stateStoreAccessPolicy;
    private final List<AsyncOutput<K, VOut>> forwarded = new ArrayList<>();

    CapturingFixedKeyProcessorContext(
            FixedKeyProcessorContext<K, VOut> delegate,
            String recordId,
            StateStoreAccessPolicy stateStoreAccessPolicy
    ) {
        this.delegate = delegate;
        this.recordId = recordId;
        this.stateStoreAccessPolicy = stateStoreAccessPolicy;
    }

    List<AsyncOutput<K, VOut>> capturedOutputs() {
        return List.copyOf(forwarded);
    }

    @Override
    public <KF extends K, VF extends VOut> void forward(FixedKeyRecord<KF, VF> record) {
        forwarded.add(new AsyncOutput<>(record.key(), record.value(), record.timestamp(), null));
    }

    @Override
    public <KF extends K, VF extends VOut> void forward(FixedKeyRecord<KF, VF> record, String childName) {
        forwarded.add(new AsyncOutput<>(record.key(), record.value(), record.timestamp(), childName));
    }

    @Override
    public <S extends StateStore> S getStateStore(String name) {
        if (stateStoreAccessPolicy != StateStoreAccessPolicy.ALLOW_UNSAFE) {
            throw new IllegalStateException(
                    "Wrapped async processors cannot access Kafka Streams state stores from worker threads by default. "
                            + "Processor recordId=" + recordId + ", store=" + name + ". "
                            + "Use StateStoreAccessPolicy.ALLOW_UNSAFE only after proving this is safe, "
                            + "or refactor the processor to snapshot state on the stream thread.");
        }
        return delegate.getStateStore(name);
    }

    @Override public String applicationId() { return delegate.applicationId(); }
    @Override public TaskId taskId() { return delegate.taskId(); }
    @Override public Optional<RecordMetadata> recordMetadata() { return delegate.recordMetadata(); }
    @Override public Serde<?> keySerde() { return delegate.keySerde(); }
    @Override public Serde<?> valueSerde() { return delegate.valueSerde(); }
    @Override public File stateDir() { return delegate.stateDir(); }
    @Override public StreamsMetrics metrics() { return delegate.metrics(); }
    @Override public Map<String, Object> appConfigs() { return delegate.appConfigs(); }
    @Override public Map<String, Object> appConfigsWithPrefix(String prefix) { return delegate.appConfigsWithPrefix(prefix); }
    @Override public long currentSystemTimeMs() { return delegate.currentSystemTimeMs(); }
    @Override public long currentStreamTimeMs() { return delegate.currentStreamTimeMs(); }

    @Override
    public Cancellable schedule(java.time.Duration interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("schedule(...) is not supported from an async wrapped processor context");
    }

    @Override
    public Cancellable schedule(java.time.Instant startTime, java.time.Duration interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("schedule(...) is not supported from an async wrapped processor context");
    }


    @Override
    public void commit() {
        // The async wrapper owns commit requests. A delegate processor may call commit(), but the
        // real synchronization point is the wrapper punctuator/commit cycle.
    }
}
