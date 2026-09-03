package kafka.streams.async;

/** Serializable representation of a Kafka record header. */
public record AsyncHeader(String key, byte[] value) {
}
