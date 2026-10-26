#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.overnight"
run_dir=$(mktemp -d "$repo_root/.overnight/smoke-step-2.XXXXXX")
compose=(docker compose -p ledgerflow-smoke -f "$repo_root/infra/docker-compose.yml")
payment_url=http://localhost:8081/api/payments

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
for dependency in docker curl jq cmp; do
  command -v "$dependency" >/dev/null || fail "Missing command: $dependency"
done

cleanup() {
  local status=$?
  trap - EXIT
  set +e
  "${compose[@]}" logs --no-color > "$run_dir/containers.log" 2>&1
  "${compose[@]}" down -v > "$run_dir/cleanup.log" 2>&1
  local cleanup_status=$?
  if (( cleanup_status != 0 )); then
    printf 'FAIL: Compose cleanup failed\n' >&2
    (( status != 0 )) || status=$cleanup_status
  fi
  if (( status == 0 )); then
    printf 'PASS: Tests run: 3, Failures: 0; stack and volumes removed\n'
  else
    printf 'Smoke check failed (exit %s).\n' "$status" >&2
  fi
  printf 'Logs: %s\n' "$run_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'Starting Compose project ledgerflow-smoke; waiting for all services to be healthy...\n'
"${compose[@]}" up -d --build --wait --wait-timeout 240 > "$run_dir/startup.log" 2>&1 \
  || fail "Stack startup failed; see $run_dir/startup.log"

post_payment() {
  local name=$1 key=$2 body=$3 status
  status=$(curl --silent --show-error --connect-timeout 5 --max-time 15 \
    -D "$run_dir/$name.headers" -o "$run_dir/$name.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $key" \
    -d "$body" "$payment_url")
  printf '%s\n' "$status" > "$run_dir/$name.status"
  [[ $status == 202 ]] || fail "$name: expected HTTP 202, got $status"
  jq -e '.id | strings | select(length > 0)' "$run_dir/$name.json" >/dev/null \
    || fail "$name: response has no payment id"
}

await_status() {
  local name=$1 expected=$2 id status attempt
  id=$(jq -r '.id' "$run_dir/$name.json")
  for ((attempt = 0; attempt < 60; attempt++)); do
    curl --fail --silent --show-error --connect-timeout 5 --max-time 5 \
      "$payment_url/$id" -o "$run_dir/$name-final.json"
    status=$(jq -er '.status' "$run_dir/$name-final.json")
    [[ $status != "$expected" ]] || return 0
    [[ $status == PENDING_LEDGER ]] || fail "$name: expected $expected, got $status"
    sleep 1
  done
  fail "$name: timed out waiting for $expected (last status: $status)"
}

# The ledger seeds customer 2 with 100000 and customer 4 with 25000 minor units.
funded_key=smoke-funded
funded_body='{"payerAccountId":2,"payeeAccountId":5,"amountMinor":2500,"currency":"USD"}'
post_payment funded "$funded_key" "$funded_body"
await_status funded COMPLETED
printf 'PASS: Funded payment reached COMPLETED\n'

post_payment insufficient smoke-insufficient \
  '{"payerAccountId":4,"payeeAccountId":5,"amountMinor":25001,"currency":"USD"}'
await_status insufficient FAILED
jq -e '.declineReason == "INSUFFICIENT_FUNDS"' "$run_dir/insufficient-final.json" >/dev/null \
  || fail 'Expected INSUFFICIENT_FUNDS decline reason'
printf 'PASS: Insufficient-funds payment reached FAILED\n'

post_payment replay "$funded_key" "$funded_body"
cmp -s "$run_dir/funded.status" "$run_dir/replay.status" || fail 'Replay HTTP status differs'
cmp -s "$run_dir/funded.json" "$run_dir/replay.json" || fail 'Replay response body differs'
grep -Eiq '^Idempotent-Replayed: true[[:space:]]*$' "$run_dir/replay.headers" \
  || fail 'Replay header missing or false'
printf 'PASS: Idempotency replay returned identical HTTP status and response body\n'
