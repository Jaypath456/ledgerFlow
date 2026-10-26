#!/usr/bin/env bash
# Short status of the LedgerFlow k3d deployment.
set -uo pipefail
K="kubectl --context k3d-ledgerflow -n ledgerflow"
k3d cluster get ledgerflow >/dev/null 2>&1 || { echo "No LedgerFlow k3d cluster. Start it with k8s/start.sh."; exit 1; }
for svc in payment:8081 ledger:8082; do
  s=$(curl -fsS -m 3 "http://localhost:${svc#*:}/actuator/health" 2>/dev/null | grep -o '"status":"[A-Z]*"' | head -1)
  printf '%-8s http://localhost:%s  %s\n' "${svc%%:*}" "${svc#*:}" "${s:-"unreachable"}"
done
echo
$K get statefulsets,deployments
echo
$K get pods -o wide
echo
$K get services,pvc
