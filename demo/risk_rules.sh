#!/usr/bin/env bash
# Payments over $10,000 and payments to a blocked account are DECLINED up front and never reach the ledger.
source "$(dirname "$0")/lib.sh"; require_stack
title "Risk rules"
cfg=$(curl -fsS "$P/demo/config")
max=$(jq .maxAmountMinor <<<"$cfg"); blocked=$(jq -r '.blockedAccounts[0] // empty' <<<"$cfg")
read -r payer payee < <(create_accounts CUSTOMER:10000 MERCHANT:0 | xargs)
pay "demo-risk-amount-$(date +%s%N)" "$(body "$payer" "$payee" $((max + 1)))" "$TMP/r1"
read -r c1 _ < "$TMP/r1.meta"; r1=$(jq -r '.status + " " + (.declineReason // "")' "$TMP/r1")
row "Over $(dollars "$max")" "HTTP $c1 $r1"
check "Amount rule: DECLINED (AMOUNT_LIMIT), HTTP 201" "[ $c1 = 201 ] && [ '$r1' = 'DECLINED AMOUNT_LIMIT' ]"
ids=$(jq -r .id "$TMP/r1")
if [ -n "$blocked" ]; then
  pay "demo-risk-blocked-$(date +%s%N)" "$(body "$payer" "$blocked" 500)" "$TMP/r2"
  read -r c2 _ < "$TMP/r2.meta"; r2=$(jq -r '.status + " " + (.declineReason // "")' "$TMP/r2")
  row "To blocked account $blocked" "HTTP $c2 $r2"
  check "Blocked rule: DECLINED (BLOCKED_ACCOUNT), HTTP 201" "[ $c2 = 201 ] && [ '$r2' = 'DECLINED BLOCKED_ACCOUNT' ]"
  ids="$ids $(jq -r .id "$TMP/r2")"
else
  row "Blocked accounts" "none configured (set RISK_BLOCKED_ACCOUNTS)"
fi
settle $ids
read -r postings mismatches _ < <(ledger_check)
row "Velocity limit in effect" "$(jq .velocityMaxPerMinute <<<"$cfg") per payer per 60 s (normal profile: 5)"
check "Declined payments never reached the ledger; payer untouched" "[ $postings = 0 ] && [ $mismatches = 0 ] && [ $(balance "$payer") = 10000 ]"
finish
