#!/usr/bin/env bash
# Deletes the LedgerFlow k3d cluster, including its PostgreSQL/Kafka volumes. Nothing else is touched.
set -euo pipefail
if k3d cluster get ledgerflow >/dev/null 2>&1; then
  k3d cluster delete ledgerflow
  echo "LedgerFlow k3d cluster deleted. Start again with k8s/start.sh."
else
  echo "No LedgerFlow k3d cluster is running."
fi
