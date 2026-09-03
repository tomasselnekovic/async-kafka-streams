package kafka.streams.async;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Helpers for creating state stores used by the async processor. */
public final class AsyncStores {
    private AsyncStores() { }

    public static <K, V> StoreBuilder<KeyValueStore<String, AsyncPendingRecord<K, V>>> pendingStore(
            String storeName,
            Serde<K> inputKeySerde,
            Serde<V> inputValueSerde
    ) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                new AsyncPendingRecordSerde<>(inputKeySerde, inputValueSerde)
        );
    }

    public static <K, V> StoreBuilder<KeyValueStore<String, AsyncOutputRecord<K, V>>> outputStore(
            String storeName,
            Serde<K> outputKeySerde,
            Serde<V> outputValueSerde
    ) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                new AsyncOutputRecordSerde<>(outputKeySerde, outputValueSerde)
        );
    }
}
