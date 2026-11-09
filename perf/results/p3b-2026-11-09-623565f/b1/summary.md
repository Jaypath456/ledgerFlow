mode steady; load generator host k6 (exit 0); dropped iterations 118; max VUs 395; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151204, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.4/5.4/26.0   request-to-terminal p50/p95/p99 ms: 34/46/112   pending after drain: 0
  kafka-1: CPU median 18.8 % max 155.7 %, memory median 665 MiB max 754 MiB
  ledger-service-1: CPU median 45.1 % max 61.1 %, memory median 445 MiB max 446 MiB
  payment-service-1: CPU median 138.6 % max 171.0 %, memory median 654 MiB max 675 MiB
  postgres-1: CPU median 108.3 % max 149.7 %, memory median 225 MiB max 246 MiB
  backlog max 77 end 59
