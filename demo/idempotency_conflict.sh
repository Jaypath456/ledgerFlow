#!/usr/bin/env bash
# The same key with a different body: HTTP 422, no second payment.
source "$(dirname "$0")/lib.sh"; require_stack
title "Idempotency conflict"
read -r payer payee < <(create_accounts CUSTOMER:10000 MERCHANT:0 | xargs)
key="demo-conflict-$(date +%s%N)"
pay "$key" "$(body "$payer" "$payee" 1000)" "$TMP/r1"; pay "$key" "$(body "$payer" "$payee" 1100)" "$TMP/r2"
read -r c1 _ < "$TMP/r1.meta"; read -r c2 _ < "$TMP/r2.meta"; id=$(jq -r .id "$TMP/r1")
settle "$id"
read -r postings mismatches _ < <(ledger_check)
amount=$(curl -fsS "$P/api/payments/$id" | jq .amountMinor); pb=$(balance "$payer")
row "First request" "$(dollars 1000) -> HTTP $c1 ($id)"
row "Second request" "$(dollars 1100), same key -> HTTP $c2: $(jq -r '.detail // "-"' "$TMP/r2")"
row "Original amount" "$(dollars "$amount")"
row "Payer balance" "$(dollars "$pb")"
check "Conflicting reuse rejected with 422" "[ $c1 = 202 ] && [ $c2 = 422 ]"
check "No payment id in the 422 response" "[ \"\$(jq -r '.id // empty' $TMP/r2)\" = '' ]"
check "Only the original was posted (\$100 -> \$90)" "[ $amount = 1000 ] && [ $pb = 9000 ] && [ $postings = 1 ] && [ $mismatches = 0 ]"
finish
