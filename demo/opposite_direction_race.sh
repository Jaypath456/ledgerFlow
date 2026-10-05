#!/usr/bin/env bash
# A and B ($100 each) pay each other 100 x $1 in both directions at once: locks in account-id order
# mean no deadlock; money is conserved. Deadlocks are read from Postgres's own counter.
source "$(dirname "$0")/lib.sh"; require_stack
N=100; AMOUNT=100
title "Opposite-direction race"
read -r a b < <(create_accounts CUSTOMER:10000 CUSTOMER:10000 | xargs)
for i in $(seq $N); do
  printf 'demo-ab-%s-%s\t%s\n' "$a" "$i" "$(body "$a" "$b" $AMOUNT)"
  printf 'demo-ba-%s-%s\t%s\n' "$b" "$i" "$(body "$b" "$a" $AMOUNT)"
done > "$TMP/requests"
d0=$(curl -fsS "$L/demo/deadlocks")
burst "$TMP/requests"
errors=$(cat "$TMP"/burst.*.meta | grep -vc '^202' || true)
ids=$(burst_ids)
settle $ids
d1=$(curl -fsS "$L/demo/deadlocks")
completed=$(status_count COMPLETED); failed=$(status_count FAILED)
read -r postings mismatches _ < <(ledger_check)
ab=$(balance "$a"); bb=$(balance "$b")
row "Requests" "$N A->B + $N B->A, concurrent"
row "Non-202 responses/timeouts" "$errors"
row "Completed / failed" "$completed / $failed"
row "Postgres deadlocks (run)" "$(( d1 - d0 ))"
row "Ending balances" "A $(dollars "$ab"), B $(dollars "$bb")"
check "Every request accepted" "[ $errors = 0 ]"
check "All $((2 * N)) COMPLETED (each side can always pay)" "[ $completed = $((2 * N)) ]"
check "No deadlocks recorded by Postgres (database-wide counter)" "[ $d1 = $d0 ]"
check "Money conserved (A + B = \$200.00)" "[ $(( ab + bb )) = 20000 ]"
check "One posting per completed payment" "[ $postings = $completed ] && [ $mismatches = 0 ]"
finish
