First baseline ramp attempt (offered 50 to 800 payments/s, 16 steps of 30 s). Kept as evidence, not the canonical ramp:
- it did not reach saturation (no failed request, achieved = offered up to 800/s), so the ramp was repeated with a higher ceiling (../ramp-2);
- its request-to-terminal columns are empty ("None") because perf/run.sh queried unqualified table names (idempotency_keys, payments) that live in the
  `payments` schema; the query was fixed before the next run. All other columns (k6, docker stats, backlog) are valid.
Code: PENDING_BASE (service code unchanged).
