package kafka.streams.async;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Durable representation of a completed async result that still has to be forwarded
 * from the Kafka Streams thread.
 */
public record AsyncOutputRecord<K, V>(
        String storeKey,
        String recordId,
        int sequence,
        K key,
        V value,
        long timestamp,
        List<AsyncHeader> headers,
        String childName,
        boolean terminalMarker,
        long createdAtEpochMs
) {
    static String storeKey(String recordId, int sequence) {
        return recordId + "\u0000" + String.format("%010d", sequence);
    }

    static String prefix(String recordId) {
        return recordId + "\u0000";
    }

    public static <K, V> AsyncOutputRecord<K, V> from(
            String recordId,
            int sequence,
            AsyncOutput<K, V> output,
            long fallbackTimestamp,
            Iterable<org.apache.kafka.common.header.Header> fallbackHeaders
    ) {
        List<AsyncHeader> copiedHeaders = new ArrayList<>();
        fallbackHeaders.forEach(h -> copiedHeaders.add(new AsyncHeader(h.key(), h.value())));
        long timestamp = output.timestamp() == null ? fallbackTimestamp : output.timestamp();
        return new AsyncOutputRecord<>(
                storeKey(recordId, sequence),
                recordId,
                sequence,
                output.key(),
                output.value(),
                timestamp,
                copiedHeaders,
                output.childName(),
                false,
                System.currentTimeMillis()
        );
    }

    public static <K, V> AsyncOutputRecord<K, V> terminal(String recordId) {
        return new AsyncOutputRecord<>(
                storeKey(recordId, 0),
                recordId,
                0,
                null,
                null,
                System.currentTimeMillis(),
                List.of(),
                null,
                true,
                System.currentTimeMillis()
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
