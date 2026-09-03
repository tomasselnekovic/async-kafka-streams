package example;

import kafka.streams.async.AsyncProcessorOptions;
import kafka.streams.async.AsyncProcessorSuppliers;
import kafka.streams.async.AsyncStores;
import kafka.streams.async.BackpressureStrategy;
import kafka.streams.async.CorrectnessMode;
import kafka.streams.async.ErrorStrategy;
import kafka.streams.async.Ordering;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/** Responsive-style wrapper example. */
public final class ResponsiveStyleWrapperExample {
    private static final String PENDING_STORE = "customer-enrichment-pending";
    private static final String OUTPUT_STORE = "customer-enrichment-output";

    public static KStream<String, EnrichedOrder> build(
            StreamsBuilder builder,
            CustomerClient customerClient,
            ExecutorService asyncExecutor,
            Serde<Order> orderSerde,
            Serde<EnrichedOrder> enrichedOrderSerde
    ) {
        builder.addStateStore(AsyncStores.pendingStore(PENDING_STORE, Serdes.String(), orderSerde));
        builder.addStateStore(AsyncStores.outputStore(OUTPUT_STORE, Serdes.String(), enrichedOrderSerde));

        AsyncProcessorOptions<String, Order, String, EnrichedOrder> options =
                AsyncProcessorOptions.<String, Order, String, EnrichedOrder>builder(asyncExecutor)
                        .processorName("customer-enrichment")
                        .pendingStoreName(PENDING_STORE)
                        .outputStoreName(OUTPUT_STORE)
                        .ordering(Ordering.KEY)
                        .maxInFlight(500)
                        .maxBufferedRecords(10_000)
                        .maxAttempts(5)
                        .initialBackoff(Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofMinutes(5))
                        .correctnessMode(CorrectnessMode.IDEMPOTENT_EXTERNAL_EFFECT)
                        .backpressureStrategy(BackpressureStrategy.FAIL)
                        .errorStrategy(ErrorStrategy.FAIL_TASK)
                        .build();

        KStream<String, EnrichedOrder> enriched = builder
                .stream("orders", Consumed.with(Serdes.String(), orderSerde))
                .processValues(
                        AsyncProcessorSuppliers.wrapValues(
                                () -> new CustomerEnrichmentProcessor(customerClient),
                                options
                        ),
                        Named.as("async-customer-enrichment"),
                        PENDING_STORE,
                        OUTPUT_STORE
                );

        enriched.to("orders-enriched", Produced.with(Serdes.String(), enrichedOrderSerde));
        return enriched;
    }

    public static final class CustomerEnrichmentProcessor implements FixedKeyProcessor<String, Order, EnrichedOrder> {
        private final CustomerClient customerClient;
        private FixedKeyProcessorContext<String, EnrichedOrder> context;

        public CustomerEnrichmentProcessor(CustomerClient customerClient) {
            this.customerClient = customerClient;
        }

        @Override
        public void init(FixedKeyProcessorContext<String, EnrichedOrder> context) {
            this.context = context;
        }

        @Override
        public void process(FixedKeyRecord<String, Order> record) {
            Order order = record.value();
            Customer customer = customerClient.fetchCustomer(order.customerId()); // slow blocking I/O
            context.forward(record.withValue(new EnrichedOrder(order.orderId(), order.customerId(), customer.segment())));
        }
    }

    public interface CustomerClient { Customer fetchCustomer(String customerId); }
    public record Order(String orderId, String customerId) { }
    public record Customer(String customerId, String segment) { }
    public record EnrichedOrder(String orderId, String customerId, String segment) { }
}
