package kafka.streams.async.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Base class for integration tests. Holds a single static {@link KafkaContainer}
 * (KRaft mode) shared across all IT subclasses to avoid repeated Docker startup costs.
 */
abstract class IntegrationTestBase {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0");

    static {
        KAFKA.start();
    }

    // ------------------------------------------------------------------ topics

    protected static void createTopics(String... topics)
            throws ExecutionException, InterruptedException, TimeoutException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> newTopics = Arrays.stream(topics)
                    .map(t -> new NewTopic(t, 1, (short) 1))
                    .toList();
            admin.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        }
    }

    // ----------------------------------------------------------------- produce

    protected static void produce(String topic, String... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) throw new IllegalArgumentException("Need even number of key-value pairs");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                producer.send(new ProducerRecord<>(topic, keyValuePairs[i], keyValuePairs[i + 1]));
            }
            producer.flush();
        }
    }

    protected static void produceN(String topic, String key, String valuePrefix, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, key, valuePrefix + i));
            }
            producer.flush();
        }
    }

    // ----------------------------------------------------------------- consume

    protected static List<ConsumerRecord<String, String>> consume(
            String topic, int expectedCount, Duration timeout) throws InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(result::add);
            }
        }
        return result;
    }

    // ----------------------------------------------------------------- streams

    /**
     * Start a {@link KafkaStreams} instance and wait until it reaches {@code RUNNING} state.
     *
     * @param topology  the topology to run
     * @param appId     unique application ID (used also as changelog topic prefix)
     * @param stateDir  local RocksDB state directory
     * @param extra     additional Streams properties
     * @return running KafkaStreams instance; caller is responsible for closing it
     */
    protected static KafkaStreams startStreams(
            Topology topology, String appId, String stateDir, Properties extra)
            throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                org.apache.kafka.common.serialization.Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "50");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        if (extra != null) props.putAll(extra);

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch running = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) running.countDown();
        });
        streams.start();
        if (!running.await(60, TimeUnit.SECONDS)) {
            streams.close();
            throw new IllegalStateException("KafkaStreams did not reach RUNNING state in 60s");
        }
        return streams;
    }

    protected static KafkaStreams startStreams(
            Topology topology, String appId, String stateDir) throws InterruptedException {
        return startStreams(topology, appId, stateDir, null);
    }

    /** Generate a unique state dir path under target/ for one test. */
    protected static String uniqueStateDir() {
        return "target/kafka-streams-it/" + UUID.randomUUID();
    }

    /** Generate a unique topic name with a given prefix. */
    protected static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Produce {@code count} records spread evenly over {@code distinctKeys} distinct keys, values "v0".."vN". */
    protected static void produceWithDistinctKeys(String topic, int distinctKeys, int totalRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < totalRecords; i++) {
                String key = "key-" + (i % distinctKeys);
                producer.send(new ProducerRecord<>(topic, key, "v" + i));
            }
            producer.flush();
        }
    }

    /** Produce exactly one record synchronously and wait for ack. */
    protected static void produceSingle(String topic, String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            try {
                producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Bulk-produce from a collection of value strings, all with the same key. */
    protected static void produceValues(String topic, String key, Collection<String> values) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String v : values) {
                producer.send(new ProducerRecord<>(topic, key, v));
            }
            producer.flush();
        }
    }
}

