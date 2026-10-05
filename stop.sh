#!/usr/bin/env bash
# Stops the LedgerFlow demo stack. Containers and data (Postgres and Kafka) are kept; ./start.sh resumes.
set -euo pipefail
cd "$(dirname "$0")"
docker compose -p ledgerflow-demo -f infra/docker-compose.yml -f infra/docker-compose.demo.yml stop
echo "LedgerFlow demo stopped (data kept). Start again with ./start.sh; wipe it with ./reset.sh."
