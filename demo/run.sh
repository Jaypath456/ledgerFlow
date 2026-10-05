#!/usr/bin/env bash
# Friendly dispatcher for the demo scenarios.   ./demo/run.sh <scenario>   ./demo/run.sh --help
set -euo pipefail
cd "$(dirname "$0")"

usage() {
  cat <<'HELP'
Usage: ./demo/run.sh <scenario>        (start the stack first: ./start.sh)

Application scenarios (fresh accounts each run, results measured and checked):
  happy               $100 payer pays $25 -> COMPLETED, posted once
  insufficient        $10 payer tries $25 -> FAILED, no ledger write
  retry               same key + body twice -> one payment, replayed response
  conflict            same key, different body -> 422, no second payment
  same-key-race       32 concurrent identical requests -> one payment, one posting
  hot-account         200 concurrent $10 payments from $500 -> 50 completed, 150 failed, $0
  opposite-direction  100 A->B and 100 B->A at once -> no deadlock, money conserved
  risk                > $10,000 and blocked-account payments are DECLINED
  invariants          full I1-I6 check of the whole database
  all                 every application scenario above, in order

Infrastructure scenarios (fault injected from the shell, under traffic):
  ledger-crash        kill -9 ledger-service and restart it
  kafka-restart       restart the Kafka broker
  postgres-pause      freeze Postgres for 10 s
HELP
}

declare -A SCRIPTS=(
  [happy]=happy_path [insufficient]=insufficient_funds [retry]=idempotent_retry [conflict]=idempotency_conflict
  [same-key-race]=same_key_race [hot-account]=hot_account_race [opposite-direction]=opposite_direction_race
  [risk]=risk_rules [invariants]=verify_invariants
  [ledger-crash]=ledger_crash_recovery [kafka-restart]=kafka_restart [postgres-pause]=postgres_pause
)

name=${1:-}
case "$name" in
  ""|-h|--help|help) usage ;;
  all)
    for s in happy insufficient retry conflict same-key-race hot-account opposite-direction risk invariants; do
      "./${SCRIPTS[$s]}.sh"
    done
    echo; echo "All application scenarios passed." ;;
  *)
    [ -n "${SCRIPTS[$name]:-}" ] || { echo "Unknown scenario: $name"; echo; usage; exit 1; }
    exec "./${SCRIPTS[$name]}.sh" ;;
esac
