Invalid diagnostic run, kept as evidence. synchronous_commit=off (ALTER SYSTEM, diagnostic only; it changes durability and is never shipped) at 1600 payments/s on
the 6-partition / 6-thread configuration: the stack collapsed (7,590 requests without response, achieved 265.9/s, payment-service CPU max 1486 %, its memory at the
1 GiB container limit, drain 136 s). Nothing can be concluded about WAL flushes from it; the retry at 1400/s is diag2-sync-off-r1400-*.
Code: 4a4e645 (service code with the configurable partitions and listener concurrency; this run used 6 and 6).
