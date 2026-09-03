package kafka.streams.async;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Serde for durable async output records. */
public final class AsyncOutputRecordSerde<K, V> implements Serde<AsyncOutputRecord<K, V>> {
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public AsyncOutputRecordSerde(Serde<K> keySerde, Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Serializer<AsyncOutputRecord<K, V>> serializer() {
        return new OutputSerializer();
    }

    @Override
    public Deserializer<AsyncOutputRecord<K, V>> deserializer() {
        return new OutputDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        keySerde.configure(configs, true);
        valueSerde.configure(configs, false);
    }

    @Override
    public void close() {
        keySerde.close();
        valueSerde.close();
    }

    private final class OutputSerializer implements Serializer<AsyncOutputRecord<K, V>> {
        @Override
        public byte[] serialize(String topic, AsyncOutputRecord<K, V> data) {
            if (data == null) return null;
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos);

                writeString(out, data.storeKey());
                writeString(out, data.recordId());
                out.writeInt(data.sequence());
                out.writeBoolean(data.terminalMarker());
                writeBytes(out, keySerde.serializer().serialize(topic, data.key()));
                writeBytes(out, valueSerde.serializer().serialize(topic, data.value()));
                out.writeLong(data.timestamp());
                writeNullableString(out, data.childName());
                out.writeLong(data.createdAtEpochMs());

                List<AsyncHeader> headers = data.headers() == null ? List.of() : data.headers();
                out.writeInt(headers.size());
                for (AsyncHeader header : headers) {
                    writeString(out, header.key());
                    writeBytes(out, header.value());
                }

                out.flush();
                return baos.toByteArray();
            } catch (IOException | RuntimeException e) {
                throw new SerializationException("Failed to serialize AsyncOutputRecord", e);
            }
        }
    }

    private final class OutputDeserializer implements Deserializer<AsyncOutputRecord<K, V>> {
        @Override
        public AsyncOutputRecord<K, V> deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

                String storeKey = readString(in);
                String recordId = readString(in);
                int sequence = in.readInt();
                boolean terminalMarker = in.readBoolean();
                byte[] keyBytes = readBytes(in);
                byte[] valueBytes = readBytes(in);
                long timestamp = in.readLong();
                String childName = readNullableString(in);
                long createdAt = in.readLong();

                int headerCount = in.readInt();
                List<AsyncHeader> headers = new ArrayList<>(headerCount);
                for (int i = 0; i < headerCount; i++) {
                    headers.add(new AsyncHeader(readString(in), readBytes(in)));
                }

                K key = keySerde.deserializer().deserialize(topic, keyBytes);
                V value = valueSerde.deserializer().deserialize(topic, valueBytes);

                return new AsyncOutputRecord<>(
                        storeKey,
                        recordId,
                        sequence,
                        key,
                        value,
                        timestamp,
                        headers,
                        childName,
                        terminalMarker,
                        createdAt
                );
            } catch (IOException | RuntimeException e) {
                throw new SerializationException("Failed to deserialize AsyncOutputRecord", e);
            }
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bytes = in.readNBytes(len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) writeString(out, value);
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) return null;
        return in.readNBytes(len);
    }
}
