# Shared helpers for demo/ scenarios. They drive the running demo stack (./start.sh) through its
# public API and the demo-profile endpoints, then CHECK the measured outcome (exit 1 on any failure).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PROJECT=ledgerflow-demo
source chaos/lib.sh
C="docker compose -p $PROJECT -f infra/docker-compose.yml -f infra/docker-compose.demo.yml"
P=$PAYMENT_URL
L=$LEDGER_URL
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
CHECKS_FAILED=0

need() { command -v "$1" >/dev/null || { echo "missing required tool: $1"; exit 2; }; }
need curl; need jq

title() { echo; echo "Scenario: $*"; echo; }
row() { printf '%-28s %s\n' "$1:" "$2"; }
dollars() { awk -v m="$1" 'BEGIN { s = m < 0 ? "-" : ""; m = m < 0 ? -m : m; printf "%s$%.2f", s, m / 100 }'; }

require_stack() {
  healthy "$P" && healthy "$L" || { echo "The demo stack is not running/healthy. Start it with ./start.sh"; exit 2; }
  curl -fsS -o /dev/null "$L/demo/invariants" 2>/dev/null \
    || { echo "Demo endpoints are not active (demo profile). Start the stack with ./start.sh"; exit 2; }
}

# create_accounts TYPE:MINOR ... -> prints the new account ids (fresh treasury each call)
create_accounts() {
  local specs="[]" s
  for s in "$@"; do specs=$(jq -c --arg t "${s%%:*}" --argjson b "${s##*:}" '. + [{type: $t, balanceMinor: $b}]' <<<"$specs"); done
  curl -fsS -X POST "$L/demo/accounts" -H 'Content-Type: application/json' -d "$specs" | jq -r '.accounts[].id'
}

balance() { curl -fsS "$L/demo/accounts?ids=$1" | jq -r '.[0].balanceMinor'; }

body() { jq -cn --argjson a "$1" --argjson b "$2" --argjson m "$3" '{payerAccountId: $a, payeeAccountId: $b, amountMinor: $m, currency: "USD"}'; }

# pay KEY BODY OUTFILE -> writes "status replayed" to OUTFILE.meta and the body to OUTFILE
pay() {
  local hdr="$3.hdr" code
  code=$(curl -sS -m 30 -o "$3" -D "$hdr" -w '%{http_code}' -X POST "$P/api/payments" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $1" -d "$2" || echo 000)
  local replayed=false
  grep -qi '^Idempotent-Replayed: true' "$hdr" 2>/dev/null && replayed=true
  echo "$code $replayed" > "$3.meta"
}

# fire every request in $1 ("KEY<TAB>BODY" per line) at once, one background curl each;
# results in $TMP/burst.<n> (+ .meta)
burst() {
  local i=0 k b
  while IFS=$'\t' read -r k b; do
    i=$((i + 1))
    pay "$k" "$b" "$TMP/burst.$i" &
  done < "$1"
  wait
}

# unique payment ids returned by the last burst
burst_ids() {
  local f
  for f in "$TMP"/burst.*; do
    case $f in *.meta|*.hdr) ;; *) jq -r '.id // empty' "$f" 2>/dev/null ;; esac
  done | sort -u
}

# settle ID... -> waits until none is PENDING_LEDGER; statuses in $TMP/statuses.json
settle() {
  local ids deadline=$(( SECONDS + ${SETTLE_TIMEOUT:-90} ))
  ids=$(printf '%s\n' "$@" | sort -u | jq -R . | jq -sc .)
  while :; do
    curl -fsS -X POST "$P/demo/payments/status" -H 'Content-Type: application/json' -d "$ids" > "$TMP/statuses.json"
    [ "$(jq '[.[] | select(.status == "PENDING_LEDGER")] | length' "$TMP/statuses.json")" = 0 ] \
      && [ "$(jq length "$TMP/statuses.json")" = "$(jq length <<<"$ids")" ] && return 0
    (( SECONDS > deadline )) && { echo "TIMEOUT: payments still PENDING_LEDGER"; return 1; }
    sleep 0.3
  done
}
status_count() { jq --arg s "$1" '[.[] | select(.status == $s)] | length' "$TMP/statuses.json"; }

# ledger cross-check for the settled payments: prints "postings mismatches duplicates"
ledger_check() {
  local ids
  ids=$(jq -c '[.[].id]' "$TMP/statuses.json")
  curl -fsS -X POST "$L/demo/payments/ledger" -H 'Content-Type: application/json' -d "$ids" > "$TMP/ledger.json"
  jq -r --slurpfile st "$TMP/statuses.json" '
    ($st[0] | map({(.id): .status}) | add) as $s
    | [ (map(.postings) | add),
        (map(select(
            ($s[.paymentId] == "COMPLETED" and (.postings != 1 or .outcome != "POSTED")) or
            ($s[.paymentId] == "FAILED"    and (.postings != 0 or .outcome != "REJECTED")) or
            ($s[.paymentId] == "DECLINED"  and (.postings != 0 or .outcome != null)))) | length),
        (map(select(.postings > 1)) | length) ] | @tsv' "$TMP/ledger.json"
}

# service-level invariant violations (ledger + payments schema, as shown in the dashboard)
service_violations() {
  echo $(( $(curl -fsS "$L/demo/invariants" | jq .violations) + $(curl -fsS "$P/demo/invariants" | jq .violations) ))
}

check() { # check "label" <condition as a test expression string>
  if eval "$2"; then echo "  ✓ $1"; else echo "  ✗ $1"; CHECKS_FAILED=$((CHECKS_FAILED + 1)); fi
}

finish() {
  local v
  v=$(service_violations)
  row "Invariant violations" "$v"
  echo
  check "Invariant check (service-level I1-I6): 0 violations" "[ $v = 0 ]"
  echo
  if [ "$CHECKS_FAILED" = 0 ]; then echo "PASS"; else echo "FAIL ($CHECKS_FAILED check(s) failed)"; exit 1; fi
}

# Full cross-schema I1-I6 check: chaos/verify_invariants.sql as the database superuser.
full_violations() {
  local v
  v=$(violations) || { echo "ERROR: invariant checker failed to run"; return 2; }
  [ -z "$v" ] && echo 0 || { echo "$v" | wc -l; echo "$v" >&2; }
}

# infra_demo "Title" "fault description" FAULT_FUNCTION
# Background traffic (one $1 payment every 100 ms for ~25 s), then the fault, then recovery and checks.
infra_demo() {
  local name=$1 fault=$2 fault_fn=$3 i code t recovery
  title "$name"
  read -r payer payee probe < <(create_accounts CUSTOMER:100000 MERCHANT:0 CUSTOMER:100000 | xargs)
  export PROBE_PAYER=$probe PROBE_PAYEE=$payee
  ( for i in $(seq 250); do pay "demo-infra-$payer-$i" "$(body "$payer" "$payee" 100)" "$TMP/t.$i"; sleep 0.1; done ) &
  local traffic=$!
  sleep 5
  row "Background traffic" "250 x \$1.00 payments, one every 100 ms"
  row "Fault" "$fault"
  "$fault_fn"
  t=$(date +%s.%N)
  recovery=$(probe_recovery "$t") || recovery=TIMEOUT
  row "Recovered after" "${recovery} s (a new payment completed end to end)"
  wait "$traffic"
  # A real client retries a failed attempt with the SAME key: that is what idempotency is for.
  local retried=0
  for i in $(seq 250); do
    read -r code _ < "$TMP/t.$i.meta"
    local n=0
    while [ "$code" != 202 ] && [ $n -lt 10 ]; do
      sleep 1; n=$((n + 1)); retried=$((retried + 1))
      pay "demo-infra-$payer-$i" "$(body "$payer" "$payee" 100)" "$TMP/t.$i"
      read -r code _ < "$TMP/t.$i.meta"
    done
  done
  local ids accepted
  accepted=$(cat "$TMP"/t.*.meta | grep -c '^202' || true)
  ids=$(for i in $(seq 250); do jq -r '.id // empty' "$TMP/t.$i" 2>/dev/null; done | sort -u)
  row "Client retries (same key)" "$retried"
  SETTLE_TIMEOUT=180 settle $ids
  local completed postings mismatches dups pb v
  completed=$(status_count COMPLETED)
  read -r postings mismatches dups < <(ledger_check)
  pb=$(balance "$payer")
  v=$(full_violations)
  row "Payments accepted" "$accepted of 250"
  row "Unique payments" "$(wc -w <<<"$ids")"
  row "Completed" "$completed"
  row "Duplicate postings" "$dups"
  row "Payer balance" "$(dollars "$pb") (expected $(dollars $(( 100000 - completed * 100 ))))"
  row "Full invariant check" "$v violations (chaos/verify_invariants.sql)"
  check "Service recovered (probe payment completed)" "[ '$recovery' != TIMEOUT ]"
  check "All 250 requests eventually accepted; one payment per key" "[ $accepted = 250 ] && [ $(wc -w <<<"$ids") = 250 ]"
  check "All 250 COMPLETED, each posted exactly once" "[ $completed = 250 ] && [ $postings = 250 ] && [ $mismatches = 0 ] && [ $dups = 0 ]"
  check "Payer debited exactly once per payment" "[ $pb = $(( 100000 - completed * 100 )) ]"
  check "Full cross-schema invariant check clean" "[ '$v' = 0 ]"
  finish
}
