package kafka.streams.async;

import org.apache.kafka.streams.processor.api.Record;

@FunctionalInterface
public interface AsyncErrorHandler<KIn, VIn> {
    void onFailure(Record<KIn, VIn> record, Throwable error, int attempts);

    static <KIn, VIn> AsyncErrorHandler<KIn, VIn> noop() {
        return (record, error, attempts) -> { };
    }
}
