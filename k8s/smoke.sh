#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.overnight"
run_dir=$(mktemp -d "$repo_root/.overnight/smoke-step-3.XXXXXX")
run_key=$(basename "$run_dir")
payment_url=http://localhost:8081/api/payments
payment_auth=${PAYMENT_AUTH:-alice:${ALICE_PASSWORD:-alice-local}}
bob_auth=${BOB_AUTH:-bob:${BOB_PASSWORD:-bob-local}}
k=(kubectl --context k3d-ledgerflow -n ledgerflow)
passed=0

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
for dependency in kubectl curl jq cmp; do
  command -v "$dependency" >/dev/null || fail "Missing command: $dependency"
done

finish() {
  local status=$?
  trap - EXIT
  set +e
  "${k[@]}" get pods -o wide > "$run_dir/pods.log" 2>&1
  "${k[@]}" get events --sort-by=.metadata.creationTimestamp > "$run_dir/events.log" 2>&1
  for app in payment-service ledger-service mongodb kafka; do
    "${k[@]}" logs -l "app=$app" --all-containers --tail=200 > "$run_dir/$app.log" 2>&1
  done
  if (( status == 0 )); then
    printf 'PASS: Tests run: 4, Failures: 0; cluster left running\n'
  else
    printf 'FAIL: Tests passed: %s/4; smoke check exited %s\n' "$passed" "$status" >&2
  fi
  printf 'Logs: %s\n' "$run_dir"
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for port in 8081 8082; do
  curl --fail --silent --show-error --connect-timeout 5 --max-time 10 \
    "http://localhost:$port/actuator/health" -o "$run_dir/health-$port.json"
  jq -e '.status == "UP"' "$run_dir/health-$port.json" >/dev/null \
    || fail "Service on port $port is unhealthy"
done

post_payment() {
  local name=$1 key=$2 body=$3 status auth=$payment_auth
  [[ $name != insufficient ]] || auth=$bob_auth
  status=$(curl --silent --show-error --connect-timeout 5 --max-time 15 \
    -u "$auth" \
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
      -u "$([[ $name = insufficient ]] && echo "$bob_auth" || echo "$payment_auth")" \
      "$payment_url/$id" -o "$run_dir/$name-final.json"
    status=$(jq -er '.status' "$run_dir/$name-final.json")
    [[ $status != "$expected" ]] || return 0
    [[ $status == PENDING_LEDGER ]] || fail "$name: expected $expected, got $status"
    sleep 1
  done
  fail "$name: timed out waiting for $expected (last status: $status)"
}

# The ledger seeds customer 2 with 100000 and customer 4 with 25000 minor units.
funded_key=$run_key-funded
funded_body='{"payerAccountId":2,"payeeAccountId":5,"amountMinor":2500,"currency":"USD"}'
post_payment funded "$funded_key" "$funded_body"
await_status funded COMPLETED
passed=$((passed + 1))
printf 'PASS: Funded payment reached COMPLETED\n'

post_payment insufficient "$run_key-insufficient" \
  '{"payerAccountId":4,"payeeAccountId":5,"amountMinor":25001,"currency":"USD"}'
await_status insufficient FAILED
jq -e '.declineReason == "INSUFFICIENT_FUNDS"' "$run_dir/insufficient-final.json" >/dev/null \
  || fail 'Expected INSUFFICIENT_FUNDS decline reason'
passed=$((passed + 1))
printf 'PASS: Insufficient-funds payment reached FAILED\n'

post_payment replay "$funded_key" "$funded_body"
cmp -s "$run_dir/funded.status" "$run_dir/replay.status" || fail 'Replay HTTP status differs'
cmp -s "$run_dir/funded.json" "$run_dir/replay.json" || fail 'Replay response body differs'
grep -Eiq '^Idempotent-Replayed: true[[:space:]]*$' "$run_dir/replay.headers" \
  || fail 'Replay header missing or false'
passed=$((passed + 1))
printf 'PASS: Idempotency replay returned identical HTTP status and response body\n'

old_pod=$("${k[@]}" get pods -l app=ledger-service -o json | jq -er '
  [.items[] | select(.metadata.deletionTimestamp == null)] |
  if length == 1 then .[0].metadata.name else error("Expected one ledger-service pod") end')
printf 'Deleting ledger-service pod %s and waiting for its replacement...\n' "$old_pod"
"${k[@]}" delete pod "$old_pod" --wait=true --timeout=120s > "$run_dir/delete-pod.log" 2>&1
new_pod=
for ((attempt = 0; attempt < 60; attempt++)); do
  new_pod=$("${k[@]}" get pods -l app=ledger-service -o json | jq -r --arg old "$old_pod" '
    [.items[] | select(.metadata.name != $old and .metadata.deletionTimestamp == null)] |
    if length == 1 then .[0].metadata.name else empty end')
  [[ -z $new_pod ]] || break
  sleep 1
done
[[ -n $new_pod ]] || fail 'No replacement ledger-service pod appeared'
"${k[@]}" wait --for=condition=Ready "pod/$new_pod" --timeout=300s > "$run_dir/replacement-ready.log" 2>&1
"${k[@]}" get "pod/$new_pod" -o json > "$run_dir/replacement-pod.json"
post_payment recovered "$run_key-recovered" "$funded_body"
await_status recovered COMPLETED
passed=$((passed + 1))
printf 'PASS: Replacement ledger-service pod %s is Ready; new payment reached COMPLETED\n' "$new_pod"
