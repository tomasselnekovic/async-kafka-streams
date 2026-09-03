package kafka.streams.async;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.streams.processor.api.Record;

/** User code: async transformation/enrichment for one input record. */
@FunctionalInterface
public interface AsyncRecordHandler<KIn, VIn, KOut, VOut> {
    CompletionStage<Collection<AsyncOutput<KOut, VOut>>> process(
            Record<KIn, VIn> record,
            AsyncRecordContext context
    );
}
