#!/usr/bin/env bash
# k8s/logs.sh payment|ledger|kafka|postgres [kubectl logs flags, e.g. -f --tail=100 --previous]
set -euo pipefail
case "${1:-}" in
  payment) w=deployment/payment-service ;;
  ledger) w=deployment/ledger-service ;;
  kafka) w=statefulset/kafka ;;
  postgres) w=statefulset/postgres ;;
  *) echo "Usage: $0 payment|ledger|kafka|postgres [kubectl logs flags]"; exit 1 ;;
esac
shift
exec kubectl --context k3d-ledgerflow -n ledgerflow logs "$w" "$@"
