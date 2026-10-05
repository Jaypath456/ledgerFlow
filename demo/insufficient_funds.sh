#!/usr/bin/env bash
# A fresh $10 payer tries to pay $25: FAILED (INSUFFICIENT_FUNDS), nothing written to the ledger.
source "$(dirname "$0")/lib.sh"; require_stack
title "Insufficient funds"
read -r payer payee < <(create_accounts CUSTOMER:1000 MERCHANT:0 | xargs)
pay "demo-insufficient-$(date +%s%N)" "$(body "$payer" "$payee" 2500)" "$TMP/r"
read -r code _ < "$TMP/r.meta"; id=$(jq -r .id "$TMP/r")
settle "$id"; status=$(jq -r '.[0].status' "$TMP/statuses.json"); reason=$(jq -r '.[0].declineReason' "$TMP/statuses.json")
read -r postings mismatches _ < <(ledger_check)
pb=$(balance "$payer"); qb=$(balance "$payee")
row "Payment" "$(dollars 2500) from a $(dollars 1000) account -> HTTP $code"
row "Final status" "$status ($reason)"
row "Balances" "payer $(dollars "$pb"), payee $(dollars "$qb")"
row "Ledger postings" "$postings"
check "FAILED with INSUFFICIENT_FUNDS" "[ $status = FAILED ] && [ $reason = INSUFFICIENT_FUNDS ]"
check "Payer still \$10.00, payee still \$0.00" "[ $pb = 1000 ] && [ $qb = 0 ]"
check "No ledger posting (no partial write)" "[ $postings = 0 ] && [ $mismatches = 0 ]"
finish
