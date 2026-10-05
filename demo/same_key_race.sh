#!/usr/bin/env bash
# 32 concurrent requests, same key + body: one logical payment, one ledger posting, 31 replays.
source "$(dirname "$0")/lib.sh"; require_stack
N=32
title "Same-key race ($N concurrent requests)"
read -r payer payee < <(create_accounts CUSTOMER:10000 MERCHANT:0 | xargs)
key="demo-race-$(date +%s%N)"; b=$(body "$payer" "$payee" 1500)
for _ in $(seq $N); do printf '%s\t%s\n' "$key" "$b"; done > "$TMP/requests"
burst "$TMP/requests"
responses=$(cat "$TMP"/burst.*.meta | grep -vc '^000' || true)
accepted=$(cat "$TMP"/burst.*.meta | grep -c '^202' || true)
replays=$(cat "$TMP"/burst.*.meta | grep -c ' true$' || true)
ids=$(burst_ids)
settle $ids
read -r postings mismatches _ < <(ledger_check)
pb=$(balance "$payer")
row "Requests sent" "$N"
row "Responses received" "$responses"
row "Unique payment IDs" "$(wc -w <<<"$ids")"
row "Replays absorbed" "$replays"
row "Ledger postings" "$postings"
row "Final status" "$(jq -r '.[0].status' "$TMP/statuses.json")"
row "Payer balance" "$(dollars "$pb")"
check "All $N answered with HTTP 202" "[ $responses = $N ] && [ $accepted = $N ]"
check "One logical payment, $((N - 1)) replays" "[ $(wc -w <<<"$ids") = 1 ] && [ $replays = $((N - 1)) ]"
check "One ledger posting, payer debited once" "[ $postings = 1 ] && [ $mismatches = 0 ] && [ $pb = 8500 ]"
finish
