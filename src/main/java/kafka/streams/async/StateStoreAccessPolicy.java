package kafka.streams.async;

/**
 * Controls whether a Responsive-style wrapped processor may access Kafka Streams state stores
 * from the async worker thread. The safe default is DISALLOW.
 */
public enum StateStoreAccessPolicy {
    /** Throw if the wrapped processor calls context.getStateStore(...). */
    DISALLOW,

    /**
     * Allow access by delegating to the real context. This is only for advanced users
     * who know their processor and stores are safe for this access pattern.
     */
    ALLOW_UNSAFE
}
