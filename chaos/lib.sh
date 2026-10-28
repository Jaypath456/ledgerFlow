# Shared helpers for bench.sh and run_chaos.sh. Source from the repo root.
PROJECT=${PROJECT:-ledgerflow-chaos}
C="docker compose -p $PROJECT -f infra/docker-compose.yml"
PAYMENT_URL=${PAYMENT_URL:-http://localhost:8081}
LEDGER_URL=${LEDGER_URL:-http://localhost:8082}
PAYMENT_AUTH=${PAYMENT_AUTH:-alice:${ALICE_PASSWORD:-alice-local}}
# The load identity owns only this explicit fixture inventory, never all ledger accounts.
export ALICE_ACCOUNTS=${ALICE_ACCOUNTS:-$(python3 -c 'print(",".join(map(str,[*range(10001,10201),30000,*range(40001,50000)])))')}

mongo() { $C exec -T mongo mongosh admin --quiet --username local_admin --password local_admin --eval "$1"; }
now_ts() { mongo 'print(new Date().toISOString())'; }

create_accounts() { # FIRST COUNT TYPE AMOUNT
  { printf "const FIRST=%s,COUNT=%s,TYPE='%s',AMOUNT=%s;\n" "$1" "$2" "$3" "$4"; cat chaos/create_accounts.js; } |
    $C exec -T mongo mongosh admin --quiet --username local_admin --password local_admin --file /dev/stdin
}

# Funded accounts: customers 10001-10200, merchants 20001-20020, probe 30000.
seed() {
  create_accounts 10001 200 CUSTOMER 1000000 >/dev/null
  create_accounts 20001 20 MERCHANT 0 >/dev/null
  create_accounts 30000 1 CUSTOMER 1000000000 >/dev/null
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
  mongo "print(db.getSiblingDB('payments').payments.countDocuments({status:'PENDING_LEDGER'})
             + db.getSiblingDB('payments').outbox.countDocuments({published_at:null})
             + db.getSiblingDB('ledger').outbox.countDocuments({published_at:null}))"
}

settle() { # seconds until nothing is pending anywhere (reconciliation may be needed)
  local start=$SECONDS
  until [ "$(backlog 2>/dev/null)" = "0" ]; do
    (( SECONDS - start > ${1:-300} )) && { echo "TIMEOUT"; return 1; }
    sleep 2
  done
  echo $(( SECONDS - start ))
}

# Prints one line per violation. Fails (non-zero) if the checker itself could not run:
# an error must never read as "0 violations".
violations() { $C exec -T mongo mongosh admin --quiet --username local_admin --password local_admin --file /dev/stdin < chaos/verify_invariants.js; }

e2e_latency_ms() { # p50/p95/p99 request-to-terminal for accepted payments created since $1
  mongo "const rows=db.getSiblingDB('payments').payments.find({created_at:{\$gte:new Date('$1')},status:{\$in:['COMPLETED','FAILED']}}).toArray();
         const ms=rows.map(p=>p.updated_at-p.created_at).sort((a,b)=>a-b);
         const percentile=q=>{if(!ms.length)return '';const i=(ms.length-1)*q;const low=Math.floor(i);return Math.round(ms[low]+(ms[Math.ceil(i)]-ms[low])*(i-low));};
         print([.50,.95,.99].map(percentile).join('/'))"
}

metric() { # current value of a Micrometer counter on a service (resets when the service restarts)
  curl -fsS -u "$PAYMENT_AUTH" "$1/actuator/metrics/$2" 2>/dev/null | jq -r '.measurements[0].value // 0' || echo 0
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
  local start=$1 key id s i
  while [ "$(elapsed_since "$start" | cut -d. -f1)" -lt 300 ]; do
    key="probe-$(date +%s%N)"
    id=$(curl -fsS -u "$PAYMENT_AUTH" -m 5 -X POST "$PAYMENT_URL/api/payments" -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
         -d "{\"payerAccountId\":${PROBE_PAYER:-30000},\"payeeAccountId\":${PROBE_PAYEE:-20001},\"amountMinor\":1,\"currency\":\"USD\"}" 2>/dev/null | jq -r '.id // empty' 2>/dev/null)
    if [ -n "$id" ]; then
      for i in $(seq 1 50); do
        s=$(curl -fsS -u "$PAYMENT_AUTH" -m 2 "$PAYMENT_URL/api/payments/$id" 2>/dev/null | jq -r '.status // empty' 2>/dev/null)
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
  docker run --rm --network host -v "$PWD/chaos:/scripts:ro" -v "$(realpath "$out"):/out" grafana/k6:latest \
    run -q --no-color -e RATE="$rate" -e DURATION="$duration" -e PAYMENT_AUTH="$PAYMENT_AUTH" -e OUT=/out/summary.json "$@" /scripts/load.js 2>&1 | grep -E '^accepted=' || true
}
