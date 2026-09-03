# Strict outbox pattern

For non-idempotent external side effects, do not execute the side effect inside the Kafka Streams processor.

Use this architecture instead:

```text
Kafka Streams topology
  -> request topic / outbox topic
  -> external worker with idempotency and retries
  -> result topic
  -> Kafka Streams topology continues
```

This is the only design in this repository that avoids pretending an arbitrary HTTP service is part of a Kafka transaction.

The async processor is still useful for idempotent reads/enrichment, but the strict outbox pattern is recommended for:

- payments
- sending emails/SMS
- mutating SaaS APIs
- external DB writes without idempotent upsert semantics
- anything where duplicate execution is unacceptable
