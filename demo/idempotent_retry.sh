#!/usr/bin/env bash
# The exact same key + body twice: one payment, one ledger effect, byte-identical replayed response.
source "$(dirname "$0")/lib.sh"; require_stack
title "Idempotent retry"
read -r payer payee < <(create_accounts CUSTOMER:10000 MERCHANT:0 | xargs)
key="demo-retry-$(date +%s%N)"; b=$(body "$payer" "$payee" 1500)
pay "$key" "$b" "$TMP/r1"; pay "$key" "$b" "$TMP/r2"
read -r c1 rp1 < "$TMP/r1.meta"; read -r c2 rp2 < "$TMP/r2.meta"
ids=$(jq -r .id "$TMP/r1" "$TMP/r2" | sort -u)
settle $ids
read -r postings mismatches _ < <(ledger_check)
pb=$(balance "$payer")
row "Requests" "2 (same key, same body)"
row "Responses" "HTTP $c1 (replayed: $rp1), HTTP $c2 (replayed: $rp2)"
row "Unique payment IDs" "$(wc -w <<<"$ids")"
row "Ledger effects" "$postings"
row "Payer balance" "$(dollars "$pb")"
check "Both HTTP 202" "[ $c1 = 202 ] && [ $c2 = 202 ]"
check "Second response is a replay" "[ $rp1 = false ] && [ $rp2 = true ]"
check "Replayed body byte-identical to the original" "cmp -s $TMP/r1 $TMP/r2"
check "One payment, one ledger posting" "[ $(wc -w <<<"$ids") = 1 ] && [ $postings = 1 ] && [ $mismatches = 0 ]"
check "Payer debited once (\$100 -> \$85)" "[ $pb = 8500 ]"
finish
