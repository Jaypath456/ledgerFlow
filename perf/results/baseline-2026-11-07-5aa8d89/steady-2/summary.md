mode steady; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151200, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.5/6.7/30.3   request-to-terminal p50/p95/p99 ms: 36/59/202   pending after drain: 0
  kafka-1: CPU median 14.7 % max 186.5 %, memory median 596 MiB max 671 MiB
  ledger-service-1: CPU median 40.9 % max 67.5 %, memory median 396 MiB max 397 MiB
  payment-service-1: CPU median 138.6 % max 204.9 %, memory median 602 MiB max 620 MiB
  postgres-1: CPU median 110.8 % max 199.4 %, memory median 224 MiB max 244 MiB
  backlog max 247 end 247
