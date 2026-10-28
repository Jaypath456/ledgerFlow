#!/usr/bin/env bash
# Fault-free load measurement against the running stack.
#   chaos/bench.sh RATE DURATION        e.g. chaos/bench.sh 50 60s
# Start and seed a new PROJECT separately for a fresh measurement; existing data is never reset.
# Prints k6 results, then backlog drain time, request-to-terminal latency and invariant violations.
set -euo pipefail
cd "$(dirname "$0")/.."
source chaos/lib.sh
rate=${1:-50}; duration=${2:-60s}
export RISK_VELOCITY_MAX=${RISK_VELOCITY_MAX:-1000000} # load must reach the ledger (DECISIONS 21)
if [ "${FRESH:-0}" = 1 ]; then
  echo 'FRESH is disabled to preserve data. Start and seed a new PROJECT instead.' >&2
  exit 1
fi
out=chaos/results/bench-$(date +%Y%m%d-%H%M%S)-r$rate
since=$(now_ts)
echo "k6: $(k6_run "$out" "$rate" "$duration")"
echo "drain_s=$(settle 300) e2e_ms_p50/p95/p99=$(e2e_latency_ms "$since")"
v=$(violations) || { echo "INVARIANT CHECKER FAILED TO RUN"; exit 1; }; echo "violations=$( [ -z "$v" ] && echo 0 || echo "$v" | wc -l)"; [ -z "$v" ] || echo "$v"
