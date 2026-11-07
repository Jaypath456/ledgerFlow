mode ramp; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 1 s; invariant violations 0
| step | offered/s | achieved/s | failed | POST p50/p95/p99 ms | e2e p50/p95/p99 ms | backlog max/end | payment CPU % med | ledger CPU % med | postgres CPU % med |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 50 | 50.0 | 0 | 3.4/5.2/7.6 | None/None/None | 4/2 | 34.7 | 17.9 | 8.6 |
| 2 | 100 | 99.2 | 0 | 2.5/3.8/5.6 | None/None/None | 8/8 | 29.4 | 12.4 | 14.1 |
| 3 | 150 | 149.2 | 0 | 2.1/3.3/4.6 | None/None/None | 12/10 | 36.5 | 15.9 | 15.5 |
| 4 | 200 | 199.2 | 0 | 1.9/3.1/4.7 | None/None/None | 16/14 | 36.5 | 11.7 | 18.6 |
| 5 | 250 | 249.2 | 0 | 1.8/2.7/3.7 | None/None/None | 20/15 | 39.2 | 13.1 | 23.6 |
| 6 | 300 | 299.2 | 0 | 1.8/2.8/3.8 | None/None/None | 24/10 | 46.2 | 14.1 | 28.2 |
| 7 | 350 | 349.2 | 0 | 1.8/2.8/4.1 | None/None/None | 26/26 | 51.1 | 15.8 | 36.2 |
| 8 | 400 | 399.2 | 0 | 1.8/3.0/5.0 | None/None/None | 34/22 | 56.8 | 17.4 | 38.5 |
| 9 | 450 | 449.2 | 0 | 1.8/3.0/5.2 | None/None/None | 34/20 | 63.9 | 18.6 | 47.0 |
| 10 | 500 | 499.2 | 0 | 1.9/3.3/5.5 | None/None/None | 36/21 | 76.0 | 21.2 | 54.9 |
| 11 | 550 | 549.2 | 0 | 1.9/3.4/5.9 | None/None/None | 40/36 | 80.7 | 23.2 | 60.4 |
| 12 | 600 | 599.2 | 0 | 2.0/3.6/6.4 | None/None/None | 42/42 | 88.6 | 25.7 | 67.2 |
| 13 | 650 | 649.2 | 0 | 2.0/3.7/8.3 | None/None/None | 50/44 | 96.9 | 27.6 | 71.0 |
| 14 | 700 | 699.2 | 0 | 2.2/4.7/15.4 | None/None/None | 115/40 | 105.4 | 29.3 | 75.5 |
| 15 | 750 | 749.2 | 0 | 2.2/4.2/7.2 | None/None/None | 57/52 | 115.2 | 32.3 | 85.2 |
| 16 | 800 | 799.1 | 0 | 2.3/5.6/41.8 | None/None/None | 75/0 | 123.8 | 38.0 | 100.5 |
first saturated step: None (offered None/s); last sustained offered: 800/s; max achieved: 799.1/s; 70 % steady target: 560/s
