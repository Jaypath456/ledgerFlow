mode steady; load generator host k6 (exit 0); dropped iterations 123637; max VUs 3000; drain 73 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 344.6/s, accepted 62025, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 8564.9/11632.2/12740.5   request-to-terminal p50/p95/p99 ms: 121706/183786/188960   pending after drain: 0
  kafka-1: CPU median 108.1 % max 278.3 %, memory median 526 MiB max 610 MiB
  ledger-service-1: CPU median 76.7 % max 242.2 %, memory median 433 MiB max 451 MiB
  payment-service-1: CPU median 349.7 % max 485.8 %, memory median 958 MiB max 975 MiB
  postgres-1: CPU median 236.2 % max 357.3 %, memory median 189 MiB max 241 MiB
  backlog max 72298 end 72298
