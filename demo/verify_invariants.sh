#!/usr/bin/env bash
# I1-I6 over the whole demo database. Runs chaos/verify_invariants.sql as the Postgres superuser
# (the only role that can read both schemas) and shows each service's own checks.
source "$(dirname "$0")/lib.sh"; require_stack
title "Invariant check (I1-I6)"
for svc in "$L" "$P"; do
  curl -fsS "$svc/demo/invariants" | jq -r '.checks[] | "\(.invariant)  \(if .pass then "PASS" else "FAIL" end)  \(.violations) violation(s)  \(.description)"'
done | sort
echo
v=$(full_violations)
row "Cross-schema SQL check" "$v violation(s) (chaos/verify_invariants.sql)"
check "Full I1-I6 check clean" "[ '$v' = 0 ]"
finish
