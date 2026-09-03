package kafka.streams.async;

/** Ordering mode for async processing. */
public enum Ordering {
    /** Max throughput, no ordering guarantees after async boundaries. */
    UNORDERED,

    /** Only one record with the same Kafka record key is processed at a time. */
    KEY
}
