package kafka.streams.async;

import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * Creates a stable id for the input record. The id is used as the key in the pending-work state store.
 */
@FunctionalInterface
public interface RecordIdStrategy<K, V> {
    String id(Record<K, V> record, Optional<RecordMetadata> metadata);

    /**
     * Stable for normal Kafka input records. Falls back to a random id for records without source metadata.
     */
    static <K, V> RecordIdStrategy<K, V> topicPartitionOffset() {
        return (record, metadata) -> metadata
            .map(m -> String.format("%s:%05d:%019d", m.topic(), m.partition(), m.offset()))
            .orElseGet(() -> "no-source-metadata:" + UUID.randomUUID());
    }
}
