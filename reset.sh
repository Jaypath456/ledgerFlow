#!/usr/bin/env bash
# Deletes ALL LedgerFlow DEMO data (Compose project "ledgerflow-demo" only: its containers, network
# and volume), then starts a fresh stack from the Flyway seed. No other project, container or
# volume is touched.   ./reset.sh [--yes]
set -euo pipefail
cd "$(dirname "$0")"

echo "This removes all LedgerFlow DEMO data (Compose project 'ledgerflow-demo': containers + volume)."
echo "Other Docker projects and volumes are not affected."
if [ "${1:-}" != "--yes" ]; then
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Cancelled."; exit 0; }
fi
docker compose -p ledgerflow-demo -f infra/docker-compose.yml -f infra/docker-compose.demo.yml down -v --remove-orphans
echo "Demo data removed. Starting a fresh stack..."
exec ./start.sh
