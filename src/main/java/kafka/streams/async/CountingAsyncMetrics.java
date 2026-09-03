package kafka.streams.async;

import java.util.concurrent.atomic.AtomicLong;

final class CountingAsyncMetrics {
    final AtomicLong received = new AtomicLong();
    final AtomicLong submitted = new AtomicLong();
    final AtomicLong succeeded = new AtomicLong();
    final AtomicLong failed = new AtomicLong();
    final AtomicLong retried = new AtomicLong();
    final AtomicLong skipped = new AtomicLong();
    final AtomicLong forwarded = new AtomicLong();
    final AtomicLong terminal = new AtomicLong();
    final AtomicLong backpressure = new AtomicLong();
}
