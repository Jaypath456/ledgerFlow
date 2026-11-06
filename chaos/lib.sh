# Shared helpers for bench.sh and run_chaos.sh. Source from the repo root.
PROJECT=${PROJECT:-ledgerflow-chaos}
C="docker compose -p $PROJECT -f infra/docker-compose.yml -f infra/docker-compose.load.yml"
PAYMENT_URL=${PAYMENT_URL:-http://localhost:8081}
LEDGER_URL=${LEDGER_URL:-http://localhost:8082}

# One generated credential per seeded payer account: load payers 10001-10200, probe 30000 and hot accounts
# 40001-40030. Identity payer-<id> owns only account <id>. Kept per project in git-ignored chaos/results/.
AUTH_FILE=chaos/results/auth-$PROJECT.json
if [ ! -s "$AUTH_FILE" ]; then
  mkdir -p chaos/results
  python3 - "$AUTH_FILE" <<'PY' || { echo "cannot create $AUTH_FILE" >&2; exit 1; }
import json, os, secrets, sys
accounts = [*range(10001, 10201), 30000, *range(40001, 40031)]
with os.fdopen(os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f:
    json.dump({str(a): secrets.token_urlsafe(18) for a in accounts}, f)
PY
fi
export LOAD_CREDENTIALS; LOAD_CREDENTIALS=$(cat "$AUTH_FILE") # read by chaos/load.js
export PAYMENT_AUTH_JSON; PAYMENT_AUTH_JSON=$(python3 -c 'import json, sys
users = [{"username": "payer-" + a, "password": p, "accounts": [int(a)]} for a, p in json.load(open(sys.argv[1])).items()]
print(json.dumps({"ledgerflow": {"auth": {"users": users}}}))' "$AUTH_FILE") # read by infra/docker-compose.load.yml
auth() { # auth ACCOUNT -> user:password for curl -u; fails if the account has no load credential
  local password
  password=$(jq -er --arg a "$1" '.[$a] // empty' "$AUTH_FILE") || { echo "no load credential for account $1" >&2; return 1; }
  printf 'payer-%s:%s' "$1" "$password"
}

sql() { $C exec -T postgres psql -U postgres -d ledgerflow -At -v ON_ERROR_STOP=1 -c "$1"; }
now_ts() { sql "SELECT now()"; }

# Funded accounts: customers 10001-10200, merchants 20001-20020, probe 30000.
seed() {
  local args="-q -v ON_ERROR_STOP=1 -f -"
  $C exec -T postgres psql -U postgres -d ledgerflow $args -v first=10001 -v count=200 -v type=CUSTOMER -v amount=1000000 < chaos/create_accounts.sql >/dev/null
  $C exec -T postgres psql -U postgres -d ledgerflow $args -v first=20001 -v count=20 -v type=MERCHANT -v amount=0 < chaos/create_accounts.sql >/dev/null
  $C exec -T postgres psql -U postgres -d ledgerflow $args -v first=30000 -v count=1 -v type=CUSTOMER -v amount=1000000000 < chaos/create_accounts.sql >/dev/null
}

healthy() { curl -fsS "$1/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; }

wait_healthy() { # seconds until both services report UP
  local start=$SECONDS
  until healthy "$PAYMENT_URL" && healthy "$LEDGER_URL"; do
    (( SECONDS - start > ${1:-300} )) && { echo "TIMEOUT"; return 1; }
    sleep 1
  done
  echo $(( SECONDS - start ))
}

backlog() { # pending payments + unpublished outbox rows in both services
  sql "SELECT (SELECT count(*) FROM payments.payments WHERE status = 'PENDING_LEDGER')
            + (SELECT count(*) FROM payments.outbox WHERE published_at IS NULL)
            + (SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL)"
}

settle() { # seconds until nothing is pending anywhere (reconciliation may be needed)
  local start=$SECONDS
  until [ "$(backlog 2>/dev/null)" = "0" ]; do
    (( SECONDS - start > ${1:-300} )) && { echo "TIMEOUT"; return 1; }
    sleep 1
  done
  echo $(( SECONDS - start ))
}

# Prints one line per violation. Fails (non-zero) if the checker itself could not run:
# an error must never read as "0 violations".
violations() { $C exec -T postgres psql -U postgres -d ledgerflow -At -F ' | ' -v ON_ERROR_STOP=1 -f - < chaos/verify_invariants.sql; }

e2e_latency_ms() { # p50/p95/p99 request-to-terminal for accepted payments created since $1
  sql "SELECT round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ms)) || '/' ||
              round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)) || '/' ||
              round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms))
       FROM (SELECT extract(epoch FROM updated_at - created_at) * 1000 AS ms FROM payments.payments
             WHERE created_at >= '$1' AND status IN ('COMPLETED', 'FAILED')) t"
}

metric() { # current value of a Micrometer counter on a service (resets when the service restarts)
  curl -fsS -u "$(auth 30000)" "$1/actuator/metrics/$2" 2>/dev/null | jq -r '.measurements[0].value // 0' || echo 0
}

dlt_total() {
  local sum=0 t
  for t in payments.requested-dlt ledger.results-dlt; do
    n=$($C exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$t" 2>/dev/null \
        | awk -F: '{s += $3} END {print s + 0}')
    sum=$(( sum + n ))
  done
  echo $sum
}

elapsed_since() { awk -v s="$1" -v e="$(date +%s.%N)" 'BEGIN { printf "%.1f", e - s }'; }

probe_recovery() { # seconds from $1 (epoch) until a new probe payment completes end to end
  # probe accounts: PROBE_PAYER/PROBE_PAYEE (default: the chaos seed's probe 30000 -> merchant 20001)
  local start=$1 key id s i credential
  credential=$(auth "${PROBE_PAYER:-30000}") || { echo TIMEOUT; return 1; }
  while [ "$(elapsed_since "$start" | cut -d. -f1)" -lt 300 ]; do
    key="probe-$(date +%s%N)"
    id=$(curl -fsS -u "$credential" -m 5 -X POST "$PAYMENT_URL/api/payments" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
         -d "{\"payerAccountId\":${PROBE_PAYER:-30000},\"payeeAccountId\":${PROBE_PAYEE:-20001},\"amountMinor\":1,\"currency\":\"USD\"}" 2>/dev/null | jq -r '.id // empty' 2>/dev/null)
    if [ -n "$id" ]; then
      for i in $(seq 1 50); do
        s=$(curl -fsS -u "$credential" -m 2 "$PAYMENT_URL/api/payments/$id" 2>/dev/null | jq -r '.status // empty' 2>/dev/null)
        [ "$s" = COMPLETED ] && { elapsed_since "$start"; return 0; }
        sleep 0.1
      done
    fi
    sleep 0.5
  done
  echo TIMEOUT; return 1
}

k6_run() { # k6_run OUT_DIR RATE DURATION [extra -e args...]
  local out=$1 rate=$2 duration=$3; shift 3
  mkdir -p "$out" && chmod 777 "$out"
  docker run --rm --memory 1g --network host -e LOAD_CREDENTIALS -v "$PWD/chaos:/scripts:ro" -v "$(realpath "$out"):/out" grafana/k6:2.3.0 \
    run -q --no-color -e RATE="$rate" -e DURATION="$duration" -e OUT=/out/summary.json "$@" /scripts/load.js 2>&1 | grep -E '^accepted=' || true
}
