package kafka.streams.async;

import java.util.Optional;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/** Context passed to the user async handler. */
public record AsyncRecordContext(
        String recordId,
        int attempt,
        Optional<RecordMetadata> recordMetadata
) {
}
