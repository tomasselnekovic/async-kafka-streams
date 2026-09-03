package kafka.streams.async;

/**
 * Output produced by an async handler. childName is optional; set it when you want to forward to a named child.
 */
public record AsyncOutput<K, V>(K key, V value, Long timestamp, String childName) {
    public static <K, V> AsyncOutput<K, V> of(K key, V value) {
        return new AsyncOutput<>(key, value, null, null);
    }

    public static <K, V> AsyncOutput<K, V> of(K key, V value, long timestamp) {
        return new AsyncOutput<>(key, value, timestamp, null);
    }

    public static <K, V> AsyncOutput<K, V> toChild(K key, V value, String childName) {
        return new AsyncOutput<>(key, value, null, childName);
    }
}
