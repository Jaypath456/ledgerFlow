#!/usr/bin/env bash
# Builds and starts the LedgerFlow demo stack (Postgres, Kafka, payment-service, ledger-service)
# as Compose project "ledgerflow-demo", waits until everything is healthy, and prints the URLs.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT=ledgerflow-demo
COMPOSE=(docker compose -p "$PROJECT" -f infra/docker-compose.yml -f infra/docker-compose.demo.yml)
PAYMENT_URL=http://localhost:8081
LEDGER_URL=http://localhost:8082

fail() { echo; echo "ERROR: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker is not installed (https://docs.docker.com/get-docker/)."
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is not available ('docker compose')."
docker info >/dev/null 2>&1 || fail "the Docker daemon is not running (or this user cannot access it)."

# Another stack (e.g. the plain Compose project or the chaos project) may already hold our ports.
for port in 8081 8082 5432 29092; do
  holder=$(docker ps --filter "publish=$port" --format '{{.Names}}' | grep -v "^$PROJECT-" || true)
  [ -z "$holder" ] || fail "port $port is used by container '$holder'. Stop that stack first, e.g.:
       docker compose -p ${holder%%-*} -f infra/docker-compose.yml stop"
done

echo "Starting LedgerFlow (first run builds the images; this can take a few minutes)..."
if ! "${COMPOSE[@]}" up -d --build --wait --wait-timeout 300; then
  echo
  "${COMPOSE[@]}" ps
  echo
  echo "Recent logs:"
  "${COMPOSE[@]}" logs --tail 40 payment-service ledger-service 2>&1 | tail -80
  fail "the stack did not become healthy. Full logs: ${COMPOSE[*]} logs"
fi

for url in "$PAYMENT_URL/actuator/health" "$LEDGER_URL/actuator/health"; do
  for _ in $(seq 1 60); do
    curl -fsS "$url" 2>/dev/null | grep -q '"status":"UP"' && continue 2
    sleep 1
  done
  fail "$url did not report UP. Logs: ${COMPOSE[*]} logs"
done

cat <<READY

LedgerFlow is ready

Dashboard:       $PAYMENT_URL/
Payment API:     $PAYMENT_URL/api/payments
Payment health:  $PAYMENT_URL/actuator/health
Ledger health:   $LEDGER_URL/actuator/health

Demo scenarios:  ./demo/run.sh --help
Stop (keep data): ./stop.sh      Reset to a clean seed: ./reset.sh
READY
