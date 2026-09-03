package kafka.streams.async;

/** Controls when the processor may start external async work. */
public enum CorrectnessMode {
    /** Lowest latency: submit async work immediately after writing the pending record. */
    FAST_IN_MEMORY_SUBMIT,

    /**
     * Safer default: write pending work first, request a commit, then submit from a later
     * punctuator/recovery scan after a configurable delay. The public Kafka Streams Processor API
     * has no commit callback, so this narrows but cannot completely eliminate the external-side
     * crash window.
     */
    STORE_FIRST_DEFERRED_SUBMIT,

    /**
     * Requires durable stores and assumes handlers use AsyncRecordContext.recordId() as an
     * idempotency key for external side effects.
     */
    IDEMPOTENT_EXTERNAL_EFFECT
}
