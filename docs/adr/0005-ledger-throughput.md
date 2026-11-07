# ADR 0005: Ledger throughput: measured bottleneck, and what more partitions did (and did not) do

Status: accepted (Roadmap Phase 3), 2026-11-07. The decision is a measured, mixed result that does not justify a new default: the defaults stay as they were.

## Context

The Phase 3 baseline (`perf/results/baseline-2026-11-07-PENDING_BASE/`) drives the unchanged services on the default Compose topology with
the cost-4 load credentials (ADR 0003) and the velocity limit raised (ADR 0002): one payment-service, one ledger-service, 3 partitions per
topic, 3 consumer threads. A ramp from 200 payments/s in steps of 100 every 30 s (`ramp-2`) sustained 1,200/s with no failed request. The first
saturated step was 1,300/s (the backlog kept growing), the best achieved rate was 1,641.8/s, and no request was ever rejected up to 2,400/s
offered. Three steady runs at 70 % of the last sustained rate (840/s) each achieved 840.0/s with 0 failed requests.

A diagnosis run just above the knee (`perf/results/diagnosis-2026-11-07-PENDING_BASE/`, 1,400/s, observability profile on, sampling 0.1) found:

- **The ledger consumer is the first stage to fall behind.** Kafka lag on `payments.requested` (consumed by ledger-service) grew from 14,876 to
  36,704 records during the run. Outbox backlogs stayed small (at most 525 rows in payment-service and 43 in ledger-service, oldest unpublished
  row under 0.5 s). In Jaeger, a payment waited a median of 14,587 ms (p95 33,839 ms) between the end of the payment relay publish and the start of
  ledger processing, while the ledger listener itself took a median of 1.0 ms.
- **The three consumer threads were saturated.** Poll idle ratio 0 on all three, 436.5, 460.7 and 449.5 records/s each, a mean listener time of
  2.27 ms per record. The listener invocation rate was 1,297.6/s (the three consumers' fetch rates add up to 1,346.7/s), which is the posting plateau.
- **They waited on the database commit path, not on row locks.** Average sessions per sample: `LWLock WALWrite` 1.43 (active); ungranted locks at
  most 1 (mean 0.18). Postgres CPU median was 265.9 %, ledger-service only 62.0 %.
- Payment-service was close behind: CPU median 329.1 %, memory median 994 MiB against its 1 GiB limit, HikariCP pool of 10 busy.

Listener concurrency (3) already equalled the partition count (3), so the roadmap's first option had nothing left to give.

## Decision

Make the partition count and the consumer thread count configurable, **with the old values as defaults**: `ledgerflow.topics.partitions`
(`TOPIC_PARTITIONS`, default 3; applies to both topics and their dead-letter topics, which need as many partitions as the topic they back) and
`spring.kafka.listener.concurrency` (`LISTENER_CONCURRENCY`, default 3). Before and after therefore run the same image.

Measure 6 partitions with 6 threads on the same ramp (`perf/results/after-2026-11-07-PENDING_FINAL/ramp`), single runs:

| | 3 partitions, 3 threads | 6 partitions, 6 threads |
|---|---|---|
| last sustained / first saturated offered rate | 1,200 / 1,300 per s | 1,300 / 1,400 per s |
| request-to-terminal p95 at 1,200 / 1,300 per s | 843 ms / 4,221 ms | 101 ms / 803 ms |
| achieved at 1,800 per s offered | 1,562.6 per s | 1,338.0 per s |
| best achieved rate | 1,641.8 per s | 1,464.0 per s |
| ledger / Postgres CPU median at the first saturated step | 62.4 % / 244.9 % | 65.4 % / 273.3 % |

So the knee moved up by one 100/s ramp step and the latency tail just below it improved, but the peak throughput and the behaviour above the knee got worse, and the
ledger CPU stayed far from saturated. With one run per configuration the step is not distinguishable from run-to-run noise, so this is **not a real gain**. The ordering
test (`PartitionedOrderingTest`) shows the configuration is correct (per-payer order, outcomes and invariants preserved), so the result is about throughput, not about
correctness. **The default stays at 3 and 3.**

## Alternatives

1. **More consumer threads without more partitions.** Useless: threads beyond the partition count stay idle.
2. **More partitions and threads (6 and 6).** Measured above; a one-step shift of the knee at best, and a lower peak, on this laptop. In a control at 1,600/s the database sessions waiting on
   `WALWrite` rose to 3.15 on average (`perf/results/diag2-sync-on-2026-11-07-PENDING_FINAL/diag/pg-wait-summary.txt`), which is consistent with commits queueing
   behind one WAL writer, but this was not isolated.
3. **`synchronous_commit = off`** would show whether the WAL flush is the shared limit, but it weakens durability for committed payments, so it is a diagnostic
   at most and never a product setting. The two diagnostic runs failed to answer the question: the host had degraded (see Consequences), and both the
   `off` run and its `on` control reached about 300/s at 1,400/s offered (293.6/s and 302.0/s). Unanswered.
4. **Batching several postings into one transaction.** Fewer commits per event, but it changes transaction boundaries around the dedupe, outcome and outbox writes
   that this phase is not allowed to touch. Not attempted. It is the next experiment if the commit path is confirmed as the limit.
5. **More hardware-like scaling (replicas).** ADR 0006.

## Consequences

- The knee of this system on this laptop is 1,200 to 1,300 payments/s. The ledger consumer falls behind first, but a larger consumer pool barely moves it: the
  limit is a resource the consumers share with payment-service (Postgres commits, and the CPU of the machine). Which one was **not isolated**.
- The two settings are kept because the scaled topology needs them (6 partitions, 6 consumers over several replicas, ADR 0006). Raising the partition count of an
  existing topic changes which partition a key maps to, so the per-payer order of events still in flight at that moment is not guaranteed; do it on a drained system.
- Measurements taken later the same day are not comparable with the morning's: with the machine under memory pressure, the same baseline steady run (840/s,
  default configuration) achieved 344.6/s instead of 840.0/s (`perf/results/drift-check-2026-11-07-PENDING_FINAL/`, host state in `host-state.txt`).
  `perf/run.sh` now records the host's swap, free memory and power state in each run's `environment.txt`.
