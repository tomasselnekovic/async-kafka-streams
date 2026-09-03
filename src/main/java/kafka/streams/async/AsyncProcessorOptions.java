package kafka.streams.async;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

/** Configuration for AsyncProcessorSupplier. */
public final class AsyncProcessorOptions<KIn, VIn, KOut, VOut> {
    private final Executor executor;
    private final int maxInFlight;
    private final int maxAttempts;
    private final int maxBufferedRecords;
    private final int outputDrainBatchSize;
    private final int recoveryScanMaxRecords;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final Duration punctuateInterval;
    private final Duration recoveryScanInterval;
    private final Duration backpressureBlockTimeout;
    private final Duration storeCommitBarrierDelay;
    private final String pendingStoreName;
    private final String outputStoreName;
    private final String processorName;
    private final RecordIdStrategy<KIn, VIn> recordIdStrategy;
    private final Ordering ordering;
    private final ErrorStrategy errorStrategy;
    private final BackpressureStrategy backpressureStrategy;
    private final CorrectnessMode correctnessMode;
    private final AsyncErrorHandler<KIn, VIn> errorHandler;
    private final AsyncMetricsListener metricsListener;
    private final StateStoreAccessPolicy stateStoreAccessPolicy;

    private AsyncProcessorOptions(Builder<KIn, VIn, KOut, VOut> b) {
        this.executor = Objects.requireNonNull(b.executor, "executor");
        this.maxInFlight = b.maxInFlight;
        this.maxAttempts = b.maxAttempts;
        this.maxBufferedRecords = b.maxBufferedRecords;
        this.outputDrainBatchSize = b.outputDrainBatchSize;
        this.recoveryScanMaxRecords = b.recoveryScanMaxRecords;
        this.initialBackoff = b.initialBackoff;
        this.maxBackoff = b.maxBackoff;
        this.punctuateInterval = b.punctuateInterval;
        this.recoveryScanInterval = b.recoveryScanInterval;
        this.backpressureBlockTimeout = b.backpressureBlockTimeout;
        this.storeCommitBarrierDelay = b.storeCommitBarrierDelay;
        this.pendingStoreName = b.pendingStoreName;
        this.outputStoreName = b.outputStoreName;
        this.processorName = b.processorName;
        this.recordIdStrategy = b.recordIdStrategy;
        this.ordering = b.ordering;
        this.errorStrategy = b.errorStrategy;
        this.backpressureStrategy = b.backpressureStrategy;
        this.correctnessMode = b.correctnessMode;
        this.errorHandler = b.errorHandler;
        this.metricsListener = b.metricsListener;
        this.stateStoreAccessPolicy = b.stateStoreAccessPolicy;
    }

    public Executor executor() { return executor; }
    public int maxInFlight() { return maxInFlight; }
    public int maxAttempts() { return maxAttempts; }
    public int maxBufferedRecords() { return maxBufferedRecords; }
    public int outputDrainBatchSize() { return outputDrainBatchSize; }
    public int recoveryScanMaxRecords() { return recoveryScanMaxRecords; }
    public Duration initialBackoff() { return initialBackoff; }
    public Duration maxBackoff() { return maxBackoff; }
    public Duration punctuateInterval() { return punctuateInterval; }
    public Duration recoveryScanInterval() { return recoveryScanInterval; }
    public Duration backpressureBlockTimeout() { return backpressureBlockTimeout; }
    public Duration storeCommitBarrierDelay() { return storeCommitBarrierDelay; }
    public String pendingStoreName() { return pendingStoreName; }
    public String outputStoreName() { return outputStoreName; }
    public String processorName() { return processorName; }
    public boolean durablePendingStoreEnabled() { return pendingStoreName != null && !pendingStoreName.isBlank(); }
    public boolean durableOutputStoreEnabled() { return outputStoreName != null && !outputStoreName.isBlank(); }
    public RecordIdStrategy<KIn, VIn> recordIdStrategy() { return recordIdStrategy; }
    public Ordering ordering() { return ordering; }
    public ErrorStrategy errorStrategy() { return errorStrategy; }
    public BackpressureStrategy backpressureStrategy() { return backpressureStrategy; }
    public CorrectnessMode correctnessMode() { return correctnessMode; }
    public AsyncErrorHandler<KIn, VIn> errorHandler() { return errorHandler; }
    public AsyncMetricsListener metricsListener() { return metricsListener; }
    public StateStoreAccessPolicy stateStoreAccessPolicy() { return stateStoreAccessPolicy; }

    public static <KIn, VIn, KOut, VOut> Builder<KIn, VIn, KOut, VOut> builder(Executor executor) {
        return new Builder<>(executor);
    }

    public static final class Builder<KIn, VIn, KOut, VOut> {
        private final Executor executor;
        private int maxInFlight = 100;
        private int maxAttempts = 3;
        private int maxBufferedRecords = 10_000;
        private int outputDrainBatchSize = 1_000;
        private int recoveryScanMaxRecords = 10_000;
        private Duration initialBackoff = Duration.ofSeconds(1);
        private Duration maxBackoff = Duration.ofMinutes(1);
        private Duration punctuateInterval = Duration.ofMillis(100);
        private Duration recoveryScanInterval = Duration.ofSeconds(5);
        private Duration backpressureBlockTimeout = Duration.ofSeconds(2);
        private Duration storeCommitBarrierDelay = Duration.ofSeconds(1);
        private String pendingStoreName;
        private String outputStoreName;
        private String processorName = "async-processor";
        private RecordIdStrategy<KIn, VIn> recordIdStrategy = RecordIdStrategy.topicPartitionOffset();
        private Ordering ordering = Ordering.KEY;
        private ErrorStrategy errorStrategy = ErrorStrategy.FAIL_TASK;
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.FAIL;
        private CorrectnessMode correctnessMode = CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT;
        private AsyncErrorHandler<KIn, VIn> errorHandler = AsyncErrorHandler.noop();
        private AsyncMetricsListener metricsListener = AsyncMetricsListener.noop();
        private StateStoreAccessPolicy stateStoreAccessPolicy = StateStoreAccessPolicy.DISALLOW;

        private Builder(Executor executor) { this.executor = executor; }

        public Builder<KIn, VIn, KOut, VOut> maxInFlight(int maxInFlight) {
            if (maxInFlight <= 0) throw new IllegalArgumentException("maxInFlight must be > 0");
            this.maxInFlight = maxInFlight;
            return this;
        }
        public Builder<KIn, VIn, KOut, VOut> maxAttempts(int maxAttempts) {
            if (maxAttempts <= 0) throw new IllegalArgumentException("maxAttempts must be > 0");
            this.maxAttempts = maxAttempts;
            return this;
        }
        public Builder<KIn, VIn, KOut, VOut> maxBufferedRecords(int maxBufferedRecords) {
            if (maxBufferedRecords <= 0) throw new IllegalArgumentException("maxBufferedRecords must be > 0");
            this.maxBufferedRecords = maxBufferedRecords;
            return this;
        }
        public Builder<KIn, VIn, KOut, VOut> outputDrainBatchSize(int outputDrainBatchSize) {
            if (outputDrainBatchSize <= 0) throw new IllegalArgumentException("outputDrainBatchSize must be > 0");
            this.outputDrainBatchSize = outputDrainBatchSize;
            return this;
        }
        public Builder<KIn, VIn, KOut, VOut> recoveryScanMaxRecords(int recoveryScanMaxRecords) {
            if (recoveryScanMaxRecords <= 0) throw new IllegalArgumentException("recoveryScanMaxRecords must be > 0");
            this.recoveryScanMaxRecords = recoveryScanMaxRecords;
            return this;
        }
        public Builder<KIn, VIn, KOut, VOut> initialBackoff(Duration initialBackoff) { this.initialBackoff = Objects.requireNonNull(initialBackoff); return this; }
        public Builder<KIn, VIn, KOut, VOut> maxBackoff(Duration maxBackoff) { this.maxBackoff = Objects.requireNonNull(maxBackoff); return this; }
        public Builder<KIn, VIn, KOut, VOut> punctuateInterval(Duration punctuateInterval) { this.punctuateInterval = Objects.requireNonNull(punctuateInterval); return this; }
        public Builder<KIn, VIn, KOut, VOut> recoveryScanInterval(Duration recoveryScanInterval) { this.recoveryScanInterval = Objects.requireNonNull(recoveryScanInterval); return this; }
        public Builder<KIn, VIn, KOut, VOut> backpressureBlockTimeout(Duration backpressureBlockTimeout) { this.backpressureBlockTimeout = Objects.requireNonNull(backpressureBlockTimeout); return this; }
        public Builder<KIn, VIn, KOut, VOut> storeCommitBarrierDelay(Duration storeCommitBarrierDelay) { this.storeCommitBarrierDelay = Objects.requireNonNull(storeCommitBarrierDelay); return this; }
        public Builder<KIn, VIn, KOut, VOut> pendingStoreName(String pendingStoreName) { this.pendingStoreName = Objects.requireNonNull(pendingStoreName); return this; }
        public Builder<KIn, VIn, KOut, VOut> outputStoreName(String outputStoreName) { this.outputStoreName = Objects.requireNonNull(outputStoreName); return this; }
        public Builder<KIn, VIn, KOut, VOut> processorName(String processorName) { this.processorName = Objects.requireNonNull(processorName); return this; }
        public Builder<KIn, VIn, KOut, VOut> recordIdStrategy(RecordIdStrategy<KIn, VIn> recordIdStrategy) { this.recordIdStrategy = Objects.requireNonNull(recordIdStrategy); return this; }
        public Builder<KIn, VIn, KOut, VOut> ordering(Ordering ordering) { this.ordering = Objects.requireNonNull(ordering); return this; }
        public Builder<KIn, VIn, KOut, VOut> errorStrategy(ErrorStrategy errorStrategy) { this.errorStrategy = Objects.requireNonNull(errorStrategy); return this; }
        public Builder<KIn, VIn, KOut, VOut> backpressureStrategy(BackpressureStrategy backpressureStrategy) { this.backpressureStrategy = Objects.requireNonNull(backpressureStrategy); return this; }
        public Builder<KIn, VIn, KOut, VOut> correctnessMode(CorrectnessMode correctnessMode) { this.correctnessMode = Objects.requireNonNull(correctnessMode); return this; }
        public Builder<KIn, VIn, KOut, VOut> errorHandler(AsyncErrorHandler<KIn, VIn> errorHandler) { this.errorHandler = Objects.requireNonNull(errorHandler); return this; }
        public Builder<KIn, VIn, KOut, VOut> metricsListener(AsyncMetricsListener metricsListener) { this.metricsListener = Objects.requireNonNull(metricsListener); return this; }
        public Builder<KIn, VIn, KOut, VOut> stateStoreAccessPolicy(StateStoreAccessPolicy stateStoreAccessPolicy) { this.stateStoreAccessPolicy = Objects.requireNonNull(stateStoreAccessPolicy); return this; }

        public AsyncProcessorOptions<KIn, VIn, KOut, VOut> build() {
            if (outputStoreName != null && pendingStoreName == null) {
                throw new IllegalStateException("outputStoreName requires pendingStoreName as well");
            }
            if ((correctnessMode == CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT
                    || correctnessMode == CorrectnessMode.IDEMPOTENT_EXTERNAL_EFFECT)
                    && (pendingStoreName == null || outputStoreName == null)) {
                throw new IllegalStateException("correctnessMode " + correctnessMode
                        + " requires both pendingStoreName and outputStoreName");
            }
            if (storeCommitBarrierDelay.isNegative()) {
                throw new IllegalStateException("storeCommitBarrierDelay must not be negative");
            }
            if (maxBufferedRecords < maxInFlight) {
                throw new IllegalStateException("maxBufferedRecords should be >= maxInFlight");
            }
            return new AsyncProcessorOptions<>(this);
        }
    }
}
