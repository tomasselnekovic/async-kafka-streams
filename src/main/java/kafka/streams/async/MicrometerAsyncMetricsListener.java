package kafka.streams.async;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public final class MicrometerAsyncMetricsListener implements AsyncMetricsListener {
    private final AtomicReference<AsyncMetricsSnapshot> snapshot = new AtomicReference<>();
    private final Counter received;
    private final Counter submitted;
    private final Counter succeeded;
    private final Counter failed;
    private final Counter retried;
    private final Counter skipped;
    private final Counter forwarded;
    private final Counter backpressure;

    public MicrometerAsyncMetricsListener(MeterRegistry registry, String processorName) {
        Objects.requireNonNull(registry, "registry");
        String[] tags = new String[] {"processor", processorName};
        this.received = Counter.builder("kafka.streams.async.records.received").tags(tags).register(registry);
        this.submitted = Counter.builder("kafka.streams.async.records.submitted").tags(tags).register(registry);
        this.succeeded = Counter.builder("kafka.streams.async.records.succeeded").tags(tags).register(registry);
        this.failed = Counter.builder("kafka.streams.async.records.failed").tags(tags).register(registry);
        this.retried = Counter.builder("kafka.streams.async.records.retried").tags(tags).register(registry);
        this.skipped = Counter.builder("kafka.streams.async.records.skipped").tags(tags).register(registry);
        this.forwarded = Counter.builder("kafka.streams.async.records.forwarded").tags(tags).register(registry);
        this.backpressure = Counter.builder("kafka.streams.async.backpressure.events").tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.inflight", snapshot, r -> value(r.get(), "inflight")).tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.scheduled", snapshot, r -> value(r.get(), "scheduled")).tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.ready.queue", snapshot, r -> value(r.get(), "ready")).tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.completed.queue", snapshot, r -> value(r.get(), "completed")).tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.pending.store.entries", snapshot, r -> value(r.get(), "pending")).tags(tags).register(registry);
        Gauge.builder("kafka.streams.async.output.store.entries", snapshot, r -> value(r.get(), "output")).tags(tags).register(registry);
    }
    @Override public void onRecordReceived(String p, String id) { received.increment(); }
    @Override public void onRecordSubmitted(String p, String id, int a) { submitted.increment(); }
    @Override public void onRecordSucceeded(String p, String id) { succeeded.increment(); }
    @Override public void onRecordFailed(String p, String id, int a, Throwable e) { failed.increment(); }
    @Override public void onRecordRetried(String p, String id, int a, long n) { retried.increment(); }
    @Override public void onRecordSkipped(String p, String id) { skipped.increment(); }
    @Override public void onRecordForwarded(String p, String id, int s) { forwarded.increment(); }
    @Override public void onBackpressure(String p, String id, BackpressureStrategy s) { backpressure.increment(); }
    @Override public void onSnapshot(String p, AsyncMetricsSnapshot s) { snapshot.set(s); }
    private static double value(AsyncMetricsSnapshot s, String f) {
        if (s == null) return 0.0;
        return switch (f) {
            case "inflight" -> s.inFlightRecords();
            case "scheduled" -> s.scheduledRecords();
            case "ready" -> s.readyQueueSize();
            case "completed" -> s.completedQueueSize();
            case "pending" -> s.pendingStoreApproxEntries();
            case "output" -> s.outputStoreApproxEntries();
            default -> 0.0;
        };
    }
}
