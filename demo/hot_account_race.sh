#!/usr/bin/env bash
# 200 concurrent $10 payments from a $500 account: exactly 50 complete, 150 fail, balance $0, no overdraft.
source "$(dirname "$0")/lib.sh"; require_stack
N=200; START=50000; AMOUNT=1000
title "Hot-account race"
read -r payer payee < <(create_accounts CUSTOMER:$START MERCHANT:0 | xargs)
b=$(body "$payer" "$payee" $AMOUNT)
for i in $(seq $N); do printf 'demo-hot-%s-%s\t%s\n' "$payer" "$i" "$b"; done > "$TMP/requests"
t0=$SECONDS
burst "$TMP/requests"
accepted=$(cat "$TMP"/burst.*.meta | grep -c '^202' || true)
ids=$(burst_ids)
settle $ids
completed=$(status_count COMPLETED); failed=$(status_count FAILED)
read -r postings mismatches dups < <(ledger_check)
pb=$(balance "$payer"); qb=$(balance "$payee")
fits=$(( START / AMOUNT ))
row "Starting balance" "$(dollars $START)"
row "Requests" "$N concurrent (unique keys)"
row "Payment amount" "$(dollars $AMOUNT)"
echo
row "Completed" "$completed"
row "Failed" "$failed"
row "Final balance" "$(dollars "$pb")"
row "Payee received" "$(dollars "$qb")"
echo
row "Negative balances" "$([ "$pb" -lt 0 ] && echo 1 || echo 0)"
row "Duplicate ledger postings" "$dups"
row "Elapsed" "$(( SECONDS - t0 )) s"
check "All $N accepted (HTTP 202), $N distinct payments" "[ $accepted = $N ] && [ $(wc -w <<<"$ids") = $N ]"
check "Exactly $fits COMPLETED, $((N - fits)) FAILED" "[ $completed = $fits ] && [ $failed = $((N - fits)) ]"
check "Final balance \$0.00, never negative" "[ $pb = $(( START - completed * AMOUNT )) ] && [ $pb -ge 0 ]"
check "Payee received exactly the completed total" "[ $qb = $(( completed * AMOUNT )) ]"
check "One posting per completed payment, no duplicates" "[ $postings = $completed ] && [ $mismatches = 0 ] && [ $dups = 0 ]"
finish
