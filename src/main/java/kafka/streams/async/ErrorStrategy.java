package kafka.streams.async;

/** What to do after retries are exhausted. */
public enum ErrorStrategy {
    /** Throw on the Kafka Streams thread, causing the task/app to fail according to your Streams uncaught-exception handling. */
    FAIL_TASK,

    /** Drop the failed record after calling the error handler. */
    SKIP
}
