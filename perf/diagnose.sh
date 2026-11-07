#!/usr/bin/env bash
# Bottleneck diagnosis helpers for a running stack (Phase 3). Used by perf/run.sh when DIAGNOSE=1; see perf/README.md.
#   perf/diagnose.sh sample OUTDIR   loop (until killed): every ~5 s, PostgreSQL activity/waits/locks and, with OBSERVABILITY=1, Prometheus instant queries
#   perf/diagnose.sh traces OUTDIR   after the load: slowest recent traces from Jaeger (OBSERVABILITY=1 only) as span-duration summaries
set -uo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh
CMD=${1:?sample|traces}; OUT=${2:?outdir}
PROM=${PROMETHEUS_URL:-http://127.0.0.1:9090}; JAEGER=${JAEGER_URL:-http://127.0.0.1:16686}
mkdir -p "$OUT"
case $CMD in
sample)
  echo "epoch,state,wait_event_type,wait_event,count" > "$OUT/pg-activity.csv"
  echo "epoch,ungranted_locks" > "$OUT/pg-locks.csv"
  : > "$OUT/prometheus.jsonl"
  QUERIES=(
    'sum by (application) (ledgerflow_outbox_backlog)'
    'max by (application) (ledgerflow_outbox_oldest_unpublished_age_seconds)'
    'max by (application, topic) (kafka_consumer_fetch_manager_records_lag_max{topic=~".+[.].+"})'
    'sum by (status) (rate(ledgerflow_ledger_outcomes_total[30s]))'
    'histogram_quantile(0.95, sum by (le) (rate(ledgerflow_payment_e2e_seconds_bucket[30s])))'
    'histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{uri="/api/payments",method="POST"}[30s])))'
    'sum by (status) (rate(http_server_requests_seconds_count{uri="/api/payments",method="POST"}[30s]))'
    'sum by (application) (hikaricp_connections_active)'
    'sum by (application) (hikaricp_connections_pending)'
    'max by (application) (hikaricp_connections_acquire_seconds_max)'
  )
  while :; do
    now=$(date +%s)
    $C exec -T postgres psql -U postgres -d ledgerflow -At -F, -c "SELECT $now, COALESCE(state,'-'), COALESCE(wait_event_type,'-'), COALESCE(wait_event,'-'), count(*) FROM pg_stat_activity WHERE datname = 'ledgerflow' AND pid <> pg_backend_pid() GROUP BY 2,3,4" >> "$OUT/pg-activity.csv" 2>/dev/null
    $C exec -T postgres psql -U postgres -d ledgerflow -At -F, -c "SELECT $now, count(*) FROM pg_locks WHERE NOT granted" >> "$OUT/pg-locks.csv" 2>/dev/null
    if [ "${OBSERVABILITY:-0}" = 1 ]; then
      for q in "${QUERIES[@]}"; do
        r=$(curl -fsS -m 4 --get "$PROM/api/v1/query" --data-urlencode "query=$q" 2>/dev/null | jq -c --arg q "$q" --argjson t "$now" '{t: $t, q: $q, r: [.data.result[] | {m: (.metric | del(.__name__, .job, .instance)), v: .value[1]}]}' 2>/dev/null) && echo "$r" >> "$OUT/prometheus.jsonl"
      done
    fi
    sleep 4
  done ;;
traces)
  [ "${OBSERVABILITY:-0}" = 1 ] || { echo "traces need OBSERVABILITY=1" >&2; exit 0; }
  END=$(date -u +%Y-%m-%dT%H:%M:%SZ); BEGIN=$(date -u -v-${TRACE_WINDOW_MIN:-6}M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "-${TRACE_WINDOW_MIN:-6} min" +%Y-%m-%dT%H:%M:%SZ)
  curl -fsS -m 60 --get "$JAEGER/api/v3/traces" --data-urlencode "query.service_name=payment-service" --data-urlencode "query.operation_name=http post /api/payments" \
    --data-urlencode "query.start_time_min=$BEGIN" --data-urlencode "query.start_time_max=$END" --data-urlencode "query.num_traces=${TRACE_COUNT:-500}" > "$OUT/jaeger-traces.json" 2>"$OUT/jaeger-traces.err" || echo "jaeger query failed" >&2
  python3 - "$OUT" <<'PY'
import json, sys, collections, statistics as st
out = sys.argv[1]
try: d = json.load(open(out + "/jaeger-traces.json"))
except Exception as e: print("no traces:", e); sys.exit(0)
traces = collections.defaultdict(dict)   # trace id -> span name -> (start ms, end ms), first span of that name
for rs in d.get("result", {}).get("resourceSpans", []):
    for ss in rs["scopeSpans"]:
        for s in ss["spans"]:
            traces[s["traceId"]].setdefault(s["name"], (int(s["startTimeUnixNano"]) / 1e6, int(s["endTimeUnixNano"]) / 1e6))
N = {"http": "http post /api/payments", "pub1": "outbox publish payments.requested", "cons1": "payments.requested process",
     "post": "ledgerflow.ledger.posting", "pub2": "outbox publish ledger.results", "cons2": "ledger.results process"}
STAGES = [("request handled (http span)", "http", "http", lambda a, b: b[1] - a[0]),
          ("payment outbox wait (http end -> relay publish start)", "http", "pub1", lambda a, b: b[0] - a[1]),
          ("payment relay publish (send + acks)", "pub1", "pub1", lambda a, b: b[1] - a[0]),
          ("Kafka + ledger consumer queue (publish end -> process start)", "pub1", "cons1", lambda a, b: b[0] - a[1]),
          ("ledger process (whole listener)", "cons1", "cons1", lambda a, b: b[1] - a[0]),
          ("  of which posting", "post", "post", lambda a, b: b[1] - a[0]),
          ("ledger outbox wait (process end -> relay publish start)", "cons1", "pub2", lambda a, b: b[0] - a[1]),
          ("ledger relay publish (send + acks)", "pub2", "pub2", lambda a, b: b[1] - a[0]),
          ("Kafka + payment consumer queue (publish end -> process start)", "pub2", "cons2", lambda a, b: b[0] - a[1]),
          ("payment result process", "cons2", "cons2", lambda a, b: b[1] - a[0]),
          ("TOTAL http start -> result process end", "http", "cons2", lambda a, b: b[1] - a[0])]
full = {t: {k: sp[v] for k, v in N.items() if v in sp} for t, sp in traces.items() if N["cons2"] in sp and N["http"] in sp}
lines = [f"{len(traces)} traces fetched, {len(full)} complete (HTTP root to payment result process)",
         "stage durations over complete traces, ms: median / p95 / max  (spans exist only for sampled requests)"]
for label, x, y, f in STAGES:
    v = sorted(f(t[x], t[y]) for t in full.values() if x in t and y in t)
    if v: lines.append(f"  {label:66s} {st.median(v):9.1f} {v[int(0.95 * (len(v) - 1))]:9.1f} {v[-1]:9.1f}  (n={len(v)})")
tot = sorted(full.items(), key=lambda kv: -(kv[1]["cons2"][1] - kv[1]["http"][0]))[:3]
lines.append("slowest 3 complete traces (stage breakdown, ms):")
for t, sp in tot:
    lines.append(f"  {t}: " + "; ".join(f"{label.split(' (')[0].strip()} {f(sp[x], sp[y]):.0f}" for label, x, y, f in STAGES if x in sp and y in sp and not label.startswith("  ")))
open(out + "/jaeger-summary.txt", "w").write("\n".join(lines) + "\n"); print("\n".join(lines))
PY
  ;;
esac
