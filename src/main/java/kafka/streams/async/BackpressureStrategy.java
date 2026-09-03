package kafka.streams.async;

/** What to do when the async processor has too much buffered or in-flight work. */
public enum BackpressureStrategy {
    FAIL,
    BLOCK,
    DROP
}
