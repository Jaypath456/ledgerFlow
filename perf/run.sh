#!/usr/bin/env bash
# Phase 3 measurement on a fresh Compose stack. See perf/README.md.
#   perf/run.sh ramp   [NAME]      stair-step ramp until saturation       (perf/results/<NAME>/)
#   perf/run.sh steady [NAME]      constant rate: RATE payments/s, WARMUP_SECONDS (excluded) + DURATION_SECONDS measured
# Env: PROJECT (default ledgerflow-p3-perf), RATE, START_RATE, STEP_RATE, STEP_SECONDS, MAX_RATE, WARMUP_SECONDS, DURATION_SECONDS,
#      TRACING_SAMPLING_PROBABILITY (default 0.1), RISK_VELOCITY_MAX (default 1000000: the load must reach the ledger, ADR 0002),
#      SCALE=1 / OBSERVABILITY=1 (chaos/lib.sh overlays), K6=host (default if k6 is installed) or K6=docker (grafana/k6:2.3.0),
#      DIAGNOSE=1 (also sample PostgreSQL waits/locks, Prometheus and Jaeger traces into <result>/diag; use with OBSERVABILITY=1),
#      RESULT_DIR (default perf/results/<NAME>), REUSE=1 (use the running stack as is), KEEP=1 (leave the stack up).
# Authentication stays on; load identities use bcrypt cost 4 (ADR 0003, infra/docker-compose.load.yml).
set -uo pipefail
cd "$(dirname "$0")/.."
MODE=${1:?usage: perf/run.sh ramp|steady [NAME]}; NAME=${2:-$MODE}
case $MODE in ramp|steady) ;; *) echo "mode must be ramp or steady" >&2; exit 2 ;; esac
export PROJECT=${PROJECT:-ledgerflow-p3-perf}
export TRACING_SAMPLING_PROBABILITY=${TRACING_SAMPLING_PROBABILITY:-0.1}
export RISK_VELOCITY_MAX=${RISK_VELOCITY_MAX:-1000000}
source chaos/lib.sh
OUT=${RESULT_DIR:-perf/results/$NAME}
[ -e "$OUT" ] && { echo "$OUT exists; choose another NAME (results are never overwritten)" >&2; exit 2; }
mkdir -p "$OUT"
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/run.log"; }
if [ "${REUSE:-0}" != 1 ] && docker ps --format '{{.Names}}' | grep -q '^ledgerflow-'; then
  echo "another ledgerflow-* stack is running; one stack at a time" >&2; docker ps --format '{{.Names}}' | grep '^ledgerflow-' >&2; rmdir "$OUT" 2>/dev/null; exit 3
fi
# --- host gate: a measured run needs a quiet machine. Refuses on battery power or more than 1 GiB of swap in use (HOST_GATE=off to override, then say so in the results) ---
host_state() {
  echo "power: $(pmset -g batt 2>/dev/null | sed -n 1p | tr -d "'")"
  echo "swap: $(sysctl -n vm.swapusage 2>/dev/null | sed 's/(encrypted)//')"
  echo "memory: $(memory_pressure 2>/dev/null | grep -i 'free percentage' | head -1); load averages $(uptime | sed 's/.*load averages*: //')"
  echo "heaviest processes by memory (MB RSS):$(ps -axo rss=,comm= | sort -rn | head -4 | awk '{printf " %s %.0f;", $2, $1/1024}' | sed 's#/[^ ;]*/##g')"
}
HOST_STATE=$(host_state)
SWAP_USED_MB=$(sysctl -n vm.swapusage 2>/dev/null | sed -E 's/.*used = ([0-9.]+)M.*/\1/')
if [ "${HOST_GATE:-on}" != off ]; then
  if echo "$HOST_STATE" | grep -q "power: Now drawing from 'Battery Power'\|power: Now drawing from Battery Power" || [ "${SWAP_USED_MB%.*}" -gt 1024 ]; then
    echo "host not quiet; refusing to measure (set HOST_GATE=off to override):" >&2; echo "$HOST_STATE" >&2; rmdir "$OUT" 2>/dev/null; exit 4
  fi
fi
RUN_ID="r$(date +%H%M%S)"
RATE=${RATE:-100}; WARMUP=${WARMUP_SECONDS:-60}; DURATION=${DURATION_SECONDS:-180}
START_RATE=${START_RATE:-50}; STEP_RATE=${STEP_RATE:-50}; STEP_SECONDS=${STEP_SECONDS:-30}; MAX_RATE=${MAX_RATE:-800}

log "start $MODE $NAME project=$PROJECT run=$RUN_ID"
if [ "${REUSE:-0}" != 1 ]; then
  $C up -d --build --wait >/dev/null 2>&1 || { log "stack failed to start"; $C down >/dev/null 2>&1; exit 1; }
  [ "$(sql "SELECT count(*) FROM ledger.accounts WHERE id = 10001")" = 0 ] && seed
fi

# --- environment, limits and settings (no credentials: the load identities and any password variable are skipped) ---
{
  echo "date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "git: $(git rev-parse --short HEAD) $(git diff --quiet HEAD -- . ':!perf/results' && echo clean || echo 'working tree differs from HEAD')"
  echo "mode: $MODE  name: $NAME  project: $PROJECT  run_id: $RUN_ID"
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null) CPUs, $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GiB RAM, $(uname -sr)"
  echo "docker: engine $(docker version --format '{{.Server.Version}}'), compose $(docker compose version --short), VM $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes')"
  echo "host state before start (host gate ${HOST_GATE:-on}):"; echo "$HOST_STATE" | sed 's/^/  /'
  echo "host state: $(pmset -g batt 2>/dev/null | sed -n 1p | tr -d "'"); swap $(sysctl -n vm.swapusage 2>/dev/null | sed 's/(encrypted)//'); $(memory_pressure 2>/dev/null | grep -i 'free percentage' | head -1); load averages $(uptime | sed 's/.*load averages*: //')"
  echo "k6: $([ "${K6:-}" = docker ] && echo 'docker grafana/k6:2.3.0 (--network host --memory 1g)' || { command -v k6 >/dev/null && echo "host $(k6 version | head -1)" || echo 'docker grafana/k6:2.3.0 (--network host --memory 1g)'; })"
  echo "compose command: ${C//-f infra/-f infra}"
  echo "settings: TRACING_SAMPLING_PROBABILITY=$TRACING_SAMPLING_PROBABILITY RISK_VELOCITY_MAX=$RISK_VELOCITY_MAX OBSERVABILITY=${OBSERVABILITY:-0} SCALE=${SCALE:-0} (load identities bcrypt cost 4)"
  if [ "$MODE" = ramp ]; then echo "load: ramp START_RATE=$START_RATE STEP_RATE=$STEP_RATE STEP_SECONDS=$STEP_SECONDS MAX_RATE=$MAX_RATE"; else echo "load: steady RATE=$RATE WARMUP_SECONDS=$WARMUP DURATION_SECONDS=$DURATION"; fi
  echo "containers:"
  for id in $($C ps -q); do
    docker inspect "$id" --format '  {{.Name}} image={{.Config.Image}} mem_limit={{.HostConfig.Memory}} nano_cpus={{.HostConfig.NanoCpus}}' | sed 's|/||'
  done
  $C config --format json 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)["services"]; keep = ("RISK_", "TRACING_", "JAVA_TOOL", "LEDGERFLOW_", "LISTENER_", "TOPIC_", "MANAGEMENT_", "KAFKA_NUM", "SPRING_PROFILES")
for n, s in sorted(d.items()):
    env = {k: v for k, v in (s.get("environment") or {}).items() if k.startswith(keep) and "PASSWORD" not in k}
    if env: print("  env", n, json.dumps(env, sort_keys=True))'
} > "$OUT/environment.txt"

# --- sampling: docker stats about every 2 s, outbox/pending backlog every ~5 s ---
echo "epoch,name,cpu,mem" > "$OUT/stats.csv"; echo "epoch,backlog" > "$OUT/backlog.csv"
( while :; do docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" 2>/dev/null | grep "^$PROJECT-" | sed "s/^/$(date +%s),/" >> "$OUT/stats.csv"; sleep 1; done ) & S1=$!
( while :; do echo "$(date +%s),$(backlog 2>/dev/null)" >> "$OUT/backlog.csv"; sleep 5; done ) & S2=$!
S3=""
if [ "${DIAGNOSE:-0}" = 1 ]; then perf/diagnose.sh sample "$OUT/diag" > /dev/null 2>&1 & S3=$!; fi   # PostgreSQL waits/locks (+ Prometheus with OBSERVABILITY=1)
trap 'kill $S1 $S2 $S3 2>/dev/null' EXIT
sleep 15   # idle settle after start; JVMs are cold, which is why steady runs exclude a warm-up window

# --- load ---
log "load start"
if [ "${K6:-}" != docker ] && command -v k6 >/dev/null; then
  K6_USED="host k6"
  MODE=$MODE RUN_ID=$RUN_ID OUT=$PWD/$OUT/k6-summary.json START_RATE=$START_RATE STEP_RATE=$STEP_RATE STEP_SECONDS=$STEP_SECONDS MAX_RATE=$MAX_RATE \
  RATE=$RATE WARMUP_SECONDS=$WARMUP DURATION_SECONDS=$DURATION k6 run -q --no-color perf/load.js > "$OUT/k6-stdout.txt" 2>&1
else
  K6_USED="docker grafana/k6:2.3.0"
  chmod 777 "$OUT"
  docker run --rm --memory 1g --network host -e LOAD_CREDENTIALS -v "$PWD/perf:/scripts:ro" -v "$(realpath "$OUT"):/out" grafana/k6:2.3.0 run -q --no-color \
    -e MODE=$MODE -e RUN_ID=$RUN_ID -e OUT=/out/k6-summary.json -e START_RATE=$START_RATE -e STEP_RATE=$STEP_RATE -e STEP_SECONDS=$STEP_SECONDS -e MAX_RATE=$MAX_RATE \
    -e RATE=$RATE -e WARMUP_SECONDS=$WARMUP -e DURATION_SECONDS=$DURATION /scripts/load.js > "$OUT/k6-stdout.txt" 2>&1
fi
K6_EXIT=$?
log "load done (exit $K6_EXIT, $K6_USED)"
LOAD_END=$(date +%s)

# --- drain, per-step request-to-terminal latency from the database, invariants ---
DRAIN=$(settle 300) || DRAIN="TIMEOUT"
log "drain_s=$DRAIN"
sql "SELECT COALESCE(json_agg(t ORDER BY step), '[]') FROM (
       SELECT substring(split_part(k.key, '-', 3) from 2)::int AS step, count(*) AS payments,
              count(*) FILTER (WHERE p.status = 'COMPLETED') AS completed, count(*) FILTER (WHERE p.status = 'FAILED') AS failed_terminal,
              count(*) FILTER (WHERE p.status = 'PENDING_LEDGER') AS pending, count(*) FILTER (WHERE p.status = 'DECLINED') AS declined,
              round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM p.updated_at - p.created_at) * 1000) FILTER (WHERE p.status IN ('COMPLETED','FAILED')))::numeric) AS e2e_p50,
              round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM p.updated_at - p.created_at) * 1000) FILTER (WHERE p.status IN ('COMPLETED','FAILED')))::numeric) AS e2e_p95,
              round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM p.updated_at - p.created_at) * 1000) FILTER (WHERE p.status IN ('COMPLETED','FAILED')))::numeric) AS e2e_p99
       FROM payments.idempotency_keys k JOIN payments.payments p ON p.id = k.payment_id
       WHERE k.key LIKE 'perf-$RUN_ID-s%' GROUP BY 1) t" > "$OUT/e2e.json"
if v=$(violations); then VIOL=$(echo -n "$v" | grep -c . || true); else VIOL="CHECKER-ERROR"; fi
log "invariant violations: $VIOL"
kill $S1 $S2 $S3 2>/dev/null
[ "${DIAGNOSE:-0}" = 1 ] && perf/diagnose.sh traces "$OUT/diag" > "$OUT/diag/traces.log" 2>&1
python3 perf/report.py "$OUT" --mode "$MODE" --drain "$DRAIN" --violations "$VIOL" --k6 "$K6_USED" --k6-exit "$K6_EXIT" \
  --step-seconds "$STEP_SECONDS" --warmup "$WARMUP" --duration "$DURATION" | tee -a "$OUT/run.log"
[ "${KEEP:-0}" = 1 ] || { $C down >/dev/null 2>&1; log "stack down (volume kept)"; }
