#!/usr/bin/env bash
# A fresh $100 payer pays $25 to a fresh payee: COMPLETED, posted once.
source "$(dirname "$0")/lib.sh"; require_stack
title "Happy path"
read -r payer payee < <(create_accounts CUSTOMER:10000 MERCHANT:0 | xargs)
pay "demo-happy-$(date +%s%N)" "$(body "$payer" "$payee" 2500)" "$TMP/r"
read -r code _ < "$TMP/r.meta"; id=$(jq -r .id "$TMP/r")
settle "$id"; status=$(jq -r '.[0].status' "$TMP/statuses.json")
read -r postings mismatches _ < <(ledger_check)
pb=$(balance "$payer"); qb=$(balance "$payee")
row "Accounts" "payer $payer ($(dollars 10000)), payee $payee ($(dollars 0))"
row "Payment" "$(dollars 2500) -> HTTP $code, $id"
row "Final status" "$status"
row "Balances" "payer $(dollars "$pb"), payee $(dollars "$qb")"
row "Ledger postings" "$postings"
check "Accepted with HTTP 202" "[ $code = 202 ]"
check "COMPLETED" "[ $status = COMPLETED ]"
check "Payer \$100 -> \$75, payee \$0 -> \$25" "[ $pb = 7500 ] && [ $qb = 2500 ]"
check "Posted exactly once" "[ $postings = 1 ] && [ $mismatches = 0 ]"
finish
