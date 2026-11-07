mode steady; load generator host k6 (exit 0); dropped iterations 184758; max VUs 3000; drain 21 s; invariant violations 0
measured window 120 s after a 45 s warm-up (excluded)
offered 1400/s, achieved 302.0/s, accepted 36244, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 9897.8/12376.3/13455.7   request-to-terminal p50/p95/p99 ms: 67204/116229/120869   pending after drain: 0
  kafka-1: CPU median 100.2 % max 257.6 %, memory median 686 MiB max 769 MiB
  ledger-service-1: CPU median 100.2 % max 206.7 %, memory median 425 MiB max 433 MiB
  payment-service-1: CPU median 315.0 % max 469.7 %, memory median 803 MiB max 975 MiB
  postgres-1: CPU median 229.9 % max 374.2 %, memory median 154 MiB max 227 MiB
  backlog max 40408 end 40408
