mode steady; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151201, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.6/6.6/19.3   request-to-terminal p50/p95/p99 ms: 36/58/142   pending after drain: 0
  kafka-1: CPU median 15.8 % max 148.2 %, memory median 593 MiB max 666 MiB
  ledger-service-1: CPU median 43.5 % max 75.5 %, memory median 401 MiB max 402 MiB
  payment-service-1: CPU median 142.8 % max 229.5 %, memory median 624 MiB max 654 MiB
  postgres-1: CPU median 116.7 % max 187.9 %, memory median 223 MiB max 242 MiB
  backlog max 106 end 60
