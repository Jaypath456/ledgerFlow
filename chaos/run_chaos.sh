#!/usr/bin/env bash
# Chaos runs against the Compose stack (project "ledgerflow-chaos", its own volume).
#   chaos/run_chaos.sh [RUNS]          default 27 = each of the 9 scenarios 3 times
# Env: LOAD_RATE (payments/s, default 100), LOAD_SECONDS (60), RESET=1 (fresh volume first),
#      KEEP_GOING=1 (do not stop at the first failed run), ONLY=a,b (subset of scenarios).
# Each run: background k6 load -> one fault -> recovery probe -> settle -> invariants (+ scenario checks).
# A run fails on any invariant violation, a checker error, a settle timeout or a failed scenario check.
set -uo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh

RUNS=${1:-27}
LOAD_RATE=${LOAD_RATE:-100}
LOAD_SECONDS=${LOAD_SECONDS:-60}
SCENARIOS=(kill-ledger kill-payment pause-postgres restart-kafka replay hot-account
           crash-before-payment-outbox-commit crash-after-publish-before-mark crash-after-ledger-commit-before-ack)
[ -n "${ONLY:-}" ] && read -ra SCENARIOS <<< "${ONLY//,/ }" # e.g. ONLY=replay,hot-account
export RISK_VELOCITY_MAX=1000000 # load and hot-account runs must reach the ledger (DECISIONS 21)

OUT=chaos/results/chaos-$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
CSV=$OUT/runs.csv
echo "run,scenario,result,k6,recovery_s,settle_s,crashes,dup_ledger,dup_payment,dlt_new,e2e_ms_p50_p95_p99,violations,note" > "$CSV"
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/chaos.log"; }

restarts() { local n=0 svc; for svc in "$@"; do n=$(( n + $(docker inspect -f '{{.RestartCount}}' "$($C ps -q "$svc")") )); done; echo $n; }

with_chaos() { # with_chaos SERVICE FAULT PROBABILITY  (recreates the service with the chaos profile)
  local up=${1^^}; up=${up%-SERVICE}
  env "${up}_PROFILES=chaos" "${up}_CHAOS_FAULTS=$2" "${up}_CHAOS_PROBABILITY=$3" $C up -d --no-deps --wait "$1" >/dev/null 2>&1
}
without_chaos() { $C up -d --no-deps --wait "$@" >/dev/null 2>&1; }

reset_group() { # reset_group GROUP TOPIC SHIFT  (group must have no running members)
  $C exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group "$1" \
    --topic "$2" --reset-offsets --shift-by "$3" --execute >/dev/null
}

# --- stack ---
[ "${RESET:-0}" = 1 ] && $C down -v >/dev/null 2>&1
log "starting stack ($PROJECT)"
$C up -d --build --wait >/dev/null 2>&1 || { log "stack failed to start"; exit 1; }
if [ "$(sql "SELECT count(*) FROM ledger.accounts WHERE id = 10001")" = 0 ]; then log "seeding accounts"; seed; fi
log "host: $(nproc) CPUs, $(free -g | awk '/Mem/ {print $2}') GiB RAM, $(uname -sr); load ${LOAD_RATE}/s for ${LOAD_SECONDS}s per run"

failures=0
for run in $(seq 1 "$RUNS"); do
  scenario=${SCENARIOS[$(( (run - 1) % ${#SCENARIOS[@]} ))]}
  note="" ok=1 crashes=0 dup_l=0 dup_p=0
  log "run $run/$RUNS: $scenario"
  dlt_before=$(dlt_total)

  case $scenario in
    crash-before-payment-outbox-commit) with_chaos payment-service BEFORE_PAYMENT_OUTBOX_COMMIT 0.0005 ;;
    crash-after-publish-before-mark)    with_chaos payment-service AFTER_OUTBOX_PUBLISH_BEFORE_MARK 0.0005
                                        with_chaos ledger-service AFTER_OUTBOX_PUBLISH_BEFORE_MARK 0.0005 ;;
    crash-after-ledger-commit-before-ack) with_chaos ledger-service AFTER_LEDGER_COMMIT_BEFORE_ACK 0.0005 ;;
  esac
  restarts_before=$(restarts payment-service ledger-service)

  since=$(now_ts)
  k6_run "$OUT/run$run" "$LOAD_RATE" "${LOAD_SECONDS}s" > "$OUT/run$run.k6" 2>&1 &
  k6_pid=$!
  sleep 15

  case $scenario in
    kill-ledger)    $C kill ledger-service >/dev/null 2>&1; sleep 5; t=$(date +%s.%N); $C start ledger-service >/dev/null 2>&1 ;;
    kill-payment)   $C kill payment-service >/dev/null 2>&1; sleep 5; t=$(date +%s.%N); $C start payment-service >/dev/null 2>&1 ;;
    pause-postgres) $C pause postgres >/dev/null 2>&1; sleep 10; t=$(date +%s.%N); $C unpause postgres >/dev/null 2>&1 ;;
    restart-kafka)  $C restart kafka >/dev/null 2>&1; t=$(date +%s.%N) ;;
    replay)
      # Re-deliver >= 1000 already-processed events to each consumer (1000 per partition x 3 partitions).
      $C stop ledger-service >/dev/null 2>&1; reset_group ledger-service payments.requested -1000
      t=$(date +%s.%N); $C start ledger-service >/dev/null 2>&1
      $C stop payment-service >/dev/null 2>&1; reset_group payment-service ledger.results -1000
      $C start payment-service >/dev/null 2>&1 ;;
    hot-account)
      # Fresh account every run (the database is cumulative across runs and campaigns).
      hot=$(sql "SELECT COALESCE(max(id), 40000) + 1 FROM ledger.accounts WHERE id BETWEEN 40001 AND 49999")
      $C exec -T postgres psql -U postgres -d ledgerflow -q -v ON_ERROR_STOP=1 -v first=$hot -v count=1 -v type=CUSTOMER \
        -v amount=5000 -f - < chaos/create_accounts.sql >/dev/null || { ok=0; note+="hot account setup failed;"; }
      t=$(date +%s.%N)
      seq 1 200 | xargs -P 200 -I{} curl -sS -m 30 -o /dev/null -X POST "$PAYMENT_URL/api/payments" \
        -H 'Content-Type: application/json' -H "Idempotency-Key: hot-$hot-{}" \
        -d "{\"payerAccountId\":$hot,\"payeeAccountId\":20001,\"amountMinor\":100,\"currency\":\"USD\"}" ;;
    crash-*) wait $k6_pid; t=$(date +%s.%N)
      crashes=$(( $(restarts payment-service ledger-service) - restarts_before ))
      without_chaos payment-service ledger-service ;;
  esac

  recovery=$(probe_recovery "$t") || { ok=0; note+="no recovery;"; }
  wait $k6_pid 2>/dev/null
  wait_healthy 300 >/dev/null || { ok=0; note+="unhealthy;"; }
  settle_s=$(settle 300) || { ok=0; note+="settle timeout;"; }

  if [ "$scenario" = replay ]; then
    until [ "$($C exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
               | awk 'NR > 1 && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}')" = 0 ]; do sleep 1; done
    # Both services were restarted for the replay, so their counters only hold replay-time duplicates.
    dup_l=$(metric "$LEDGER_URL" ledgerflow.events.duplicate | cut -d. -f1)
    dup_p=$(metric "$PAYMENT_URL" ledgerflow.events.duplicate | cut -d. -f1)
    [ "$dup_l" -ge 1000 ] && [ "$dup_p" -ge 1000 ] || { ok=0; note+="replay absorbed too few duplicates;"; }
  else
    dup_l=$(metric "$LEDGER_URL" ledgerflow.events.duplicate | cut -d. -f1)
    dup_p=$(metric "$PAYMENT_URL" ledgerflow.events.duplicate | cut -d. -f1)
  fi

  if [ "$scenario" = hot-account ]; then
    r=$(sql "SELECT count(*) FILTER (WHERE status = 'COMPLETED') || '/' || count(*) FILTER (WHERE status = 'FAILED') || '/' || count(*)
             FROM payments.payments WHERE payer_account_id = $hot")
    bal=$(sql "SELECT balance_minor FROM ledger.accounts WHERE id = $hot")
    note+="hot completed/failed/total=$r balance=$bal;"
    [ "$r" = "50/150/200" ] && [ "$bal" = 0 ] || { ok=0; note+="hot account check FAILED;"; }
  fi

  if v=$(violations); then
    nviol=$(echo -n "$v" | grep -c . || true)
    [ "$nviol" = 0 ] || { ok=0; echo "$v" > "$OUT/run$run.violations"; }
  else
    ok=0; nviol=ERROR; note+="checker failed to run;"
  fi

  result=$([ $ok = 1 ] && echo PASS || echo FAIL)
  dlt_new=$(( $(dlt_total) - dlt_before ))
  echo "$run,$scenario,$result,\"$(cat "$OUT/run$run.k6")\",$recovery,$settle_s,$crashes,$dup_l,$dup_p,$dlt_new,$(e2e_latency_ms "$since"),$nviol,\"$note\"" >> "$CSV"
  log "run $run $result recovery=${recovery}s settle=${settle_s}s crashes=$crashes dup=$dup_l/$dup_p dlt_new=$dlt_new violations=$nviol $note"
  if [ $ok = 0 ]; then
    failures=$(( failures + 1 ))
    [ "${KEEP_GOING:-0}" = 1 ] || { log "stopping at first failure; state kept for investigation"; break; }
  fi
done

log "done: $(grep -c ',PASS,' "$CSV") passed, $failures failed; results in $CSV"
[ $failures = 0 ]
