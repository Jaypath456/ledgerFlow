#!/usr/bin/env bash
# Pod recovery demo on the k3d deployment: steady payment traffic, `kubectl delete pod` on ledger-service,
# Kubernetes starts a replacement, payments converge, and every payment is checked for exactly-once posting.
# Reuses the demo/ scenario helpers; only the fault and the database access are Kubernetes-specific.
source "$(dirname "$0")/../demo/lib.sh"
K="kubectl --context k3d-ledgerflow -n ledgerflow"
need kubectl
violations() { $K exec -i postgres-0 -- psql -U postgres -d ledgerflow -At -F ' | ' -v ON_ERROR_STOP=1 -f - < chaos/verify_invariants.sql; }

ready=$($K get pods --no-headers 2>/dev/null | awk '$2 ~ /^1\/1$/ && $3 == "Running"' | wc -l)
[ "$ready" = 4 ] || { echo "Expected 4 ready pods, found $ready. Start the cluster with k8s/start.sh"; exit 2; }
require_stack

pods() { $K get pods -l app=ledger-service -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'; }

delete_ledger_pod() {
  local old new t
  old=$(pods)
  t=$(date +%s.%N)
  $K delete pod "$old" --wait=false >/dev/null
  until new=$(pods | grep -vx "$old"); do sleep 0.1; done
  row "Deleted pod" "$old (graceful SIGTERM)"
  row "Replacement pod" "$new, created after $(elapsed_since "$t") s"
  $K wait --for=condition=Ready "pod/$new" --timeout=300s >/dev/null
  row "Replacement READY after" "$(elapsed_since "$t") s"
}

infra_demo "Kubernetes pod recovery (ledger-service)" "kubectl delete pod <ledger-service pod>" delete_ledger_pod
