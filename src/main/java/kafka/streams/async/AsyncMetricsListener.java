package kafka.streams.async;

public interface AsyncMetricsListener {
    default void onRecordReceived(String processorName, String recordId) { }
    default void onRecordSubmitted(String processorName, String recordId, int attempt) { }
    default void onRecordSucceeded(String processorName, String recordId) { }
    default void onRecordFailed(String processorName, String recordId, int attempt, Throwable error) { }
    default void onRecordRetried(String processorName, String recordId, int nextAttempt, long notBeforeEpochMs) { }
    default void onRecordSkipped(String processorName, String recordId) { }
    default void onRecordForwarded(String processorName, String recordId, int sequence) { }
    default void onBackpressure(String processorName, String recordId, BackpressureStrategy strategy) { }
    default void onSnapshot(String processorName, AsyncMetricsSnapshot snapshot) { }
    static AsyncMetricsListener noop() { return new AsyncMetricsListener() { }; }
}
