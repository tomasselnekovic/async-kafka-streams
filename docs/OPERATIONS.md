# Operations guide

Alert on:

```text
pending store entries increasing continuously
output store entries increasing continuously
in-flight records pinned at maxInFlight
backpressure events > 0
retry rate increasing
failed records > 0
completed queue not draining
```

Tuning rules:

```text
maxInFlight <= downstream safe parallelism
maxBufferedRecords >= maxInFlight * 10
outputDrainBatchSize high enough to drain completions, low enough not to monopolize punctuation
recoveryScanMaxRecords high enough for restart recovery, low enough not to block the stream thread too long
```

For OpenShift:

```text
scale vertically first if the bottleneck is async callback or store IO
scale partitions/instances only if partition assignment permits it
keep idempotency enabled in downstream systems
```
