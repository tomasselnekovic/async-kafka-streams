# Crash-safety model

The library reduces the dangerous windows by persisting input before async execution and persisting output before forwarding.

## Covered

```text
crash after input is received but before async work starts
  -> pending store restores the input

crash while async work is in flight
  -> pending store restores the input and retries it

crash after completion was persisted but before forwarding
  -> output store restores the output and forwards it
```

## Not fully covered

```text
remote service succeeds, then JVM dies before the completion is stored
  -> pending store retries the input
  -> remote service may see duplicate request
```

Therefore external calls must be idempotent.

## Strongest recommended config

```properties
processing.guarantee=exactly_once_v2
commit.interval.ms=100
```

This strengthens Kafka state/output consistency, but it still does not make remote systems transactional with Kafka.
