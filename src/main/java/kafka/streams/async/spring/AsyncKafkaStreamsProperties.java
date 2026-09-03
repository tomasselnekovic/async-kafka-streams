package kafka.streams.async.spring;

import kafka.streams.async.CorrectnessMode;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka.streams.async")
public class AsyncKafkaStreamsProperties {
    /** Size of the default executor created by the auto-configuration. */
    private int executorThreads = 32;
    private String processorName = "async-processor";
    private int maxInFlight = 100;
    private int maxBufferedRecords = 10_000;
    private int outputDrainBatchSize = 1_000;
    private int recoveryScanMaxRecords = 10_000;
    private Duration punctuateInterval = Duration.ofMillis(100);
    private Duration recoveryScanInterval = Duration.ofSeconds(5);
    private Duration storeCommitBarrierDelay = Duration.ofSeconds(1);
    private CorrectnessMode correctnessMode = CorrectnessMode.STORE_FIRST_DEFERRED_SUBMIT;

    public int getExecutorThreads() { return executorThreads; }
    public void setExecutorThreads(int executorThreads) { this.executorThreads = executorThreads; }
    public String getProcessorName() { return processorName; }
    public void setProcessorName(String processorName) { this.processorName = processorName; }
    public int getMaxInFlight() { return maxInFlight; }
    public void setMaxInFlight(int maxInFlight) { this.maxInFlight = maxInFlight; }
    public int getMaxBufferedRecords() { return maxBufferedRecords; }
    public void setMaxBufferedRecords(int maxBufferedRecords) { this.maxBufferedRecords = maxBufferedRecords; }
    public int getOutputDrainBatchSize() { return outputDrainBatchSize; }
    public void setOutputDrainBatchSize(int outputDrainBatchSize) { this.outputDrainBatchSize = outputDrainBatchSize; }
    public int getRecoveryScanMaxRecords() { return recoveryScanMaxRecords; }
    public void setRecoveryScanMaxRecords(int recoveryScanMaxRecords) { this.recoveryScanMaxRecords = recoveryScanMaxRecords; }
    public Duration getPunctuateInterval() { return punctuateInterval; }
    public void setPunctuateInterval(Duration punctuateInterval) { this.punctuateInterval = punctuateInterval; }
    public Duration getRecoveryScanInterval() { return recoveryScanInterval; }
    public void setRecoveryScanInterval(Duration recoveryScanInterval) { this.recoveryScanInterval = recoveryScanInterval; }
    public Duration getStoreCommitBarrierDelay() { return storeCommitBarrierDelay; }
    public void setStoreCommitBarrierDelay(Duration storeCommitBarrierDelay) { this.storeCommitBarrierDelay = storeCommitBarrierDelay; }
    public CorrectnessMode getCorrectnessMode() { return correctnessMode; }
    public void setCorrectnessMode(CorrectnessMode correctnessMode) { this.correctnessMode = correctnessMode; }
}

