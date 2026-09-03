package kafka.streams.async;

public record AsyncMetricsSnapshot(
        long receivedRecords,
        long submittedRecords,
        long succeededRecords,
        long failedRecords,
        long retriedRecords,
        long skippedRecords,
        long forwardedRecords,
        long terminalRecords,
        long backpressureEvents,
        int inFlightRecords,
        int scheduledRecords,
        int readyQueueSize,
        int completedQueueSize,
        long pendingStoreApproxEntries,
        long outputStoreApproxEntries
) { }
