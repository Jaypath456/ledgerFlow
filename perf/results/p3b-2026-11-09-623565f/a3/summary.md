mode steady; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151201, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.3/4.7/13.7   request-to-terminal p50/p95/p99 ms: 34/47/84   pending after drain: 0
  kafka-1: CPU median 13.0 % max 146.3 %, memory median 604 MiB max 693 MiB
  ledger-service-1: CPU median 36.7 % max 56.1 %, memory median 400 MiB max 401 MiB
  payment-service-1: CPU median 128.9 % max 167.0 %, memory median 582 MiB max 594 MiB
  postgres-1: CPU median 100.6 % max 160.5 %, memory median 222 MiB max 344 MiB
  backlog max 102 end 67
