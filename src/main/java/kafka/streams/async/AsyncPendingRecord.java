package kafka.streams.async;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Durable representation of an input record that has not yet completed async processing.
 */
public record AsyncPendingRecord<K, V>(
        String recordId,
        K key,
        V value,
        long timestamp,
        List<AsyncHeader> headers,
        int attempt,
        long notBeforeEpochMs,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String lastError
) {
    public static <K, V> AsyncPendingRecord<K, V> from(
            String recordId,
            Record<K, V> record,
            int attempt,
            long notBeforeEpochMs
    ) {
        List<AsyncHeader> copiedHeaders = new ArrayList<>();
        record.headers().forEach(h -> copiedHeaders.add(new AsyncHeader(h.key(), h.value())));
        long now = System.currentTimeMillis();
        return new AsyncPendingRecord<>(
                recordId,
                record.key(),
                record.value(),
                record.timestamp(),
                copiedHeaders,
                attempt,
                notBeforeEpochMs,
                now,
                now,
                null
        );
    }

    /** Pending rows key on the record id itself. */
    public String storeKey() {
        return recordId;
    }

    public AsyncPendingRecord<K, V> retry(int nextAttempt, long nextNotBeforeEpochMs, Throwable error) {
        return new AsyncPendingRecord<>(
                recordId,
                key,
                value,
                timestamp,
                headers,
                nextAttempt,
                nextNotBeforeEpochMs,
                createdAtEpochMs,
                System.currentTimeMillis(),
                error == null ? null : error.toString()
        );
    }

    public Record<K, V> toRecord() {
        RecordHeaders recordHeaders = new RecordHeaders();
        if (headers != null) {
            for (AsyncHeader header : headers) {
                recordHeaders.add(header.key(), header.value());
            }
        }
        return new Record<>(key, value, timestamp, recordHeaders);
    }
}
