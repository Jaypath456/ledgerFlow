#!/usr/bin/env bash
# Starts LedgerFlow on a local single-node k3d cluster "ledgerflow": builds both images, imports them,
# applies infra/k8s/, initializes MongoDB rs0, waits for Kafka and both services, and prints the URLs.
set -euo pipefail
cd "$(dirname "$0")/.."
started=$SECONDS

CLUSTER=ledgerflow
CTX=k3d-$CLUSTER
K="kubectl --context $CTX -n ledgerflow"
PAYMENT_IMAGE=ledgerflow-payment:1.1.0-local
LEDGER_IMAGE=ledgerflow-ledger:1.1.0-local

fail() { echo; echo "ERROR: $*" >&2; exit 1; }
step() { printf '[%3ss] %s\n' $(( SECONDS - started )) "$*"; }

for tool in docker kubectl k3d curl; do
  command -v $tool >/dev/null 2>&1 || fail "$tool is not installed."
done
docker info >/dev/null 2>&1 || fail "the Docker daemon is not running (or this user cannot access it)."

exists=false
k3d cluster get $CLUSTER >/dev/null 2>&1 && exists=true

# 8081/8082 must be free, or already published by this cluster's own node container.
for port in 8081 8082; do
  holder=$(docker ps --filter "publish=$port" --format '{{.Names}}' | grep -v "^k3d-$CLUSTER-" || true)
  [ -z "$holder" ] || fail "port $port is used by container '$holder'. Stop that stack first."
  if ! $exists; then
    if command -v lsof >/dev/null 2>&1; then
      ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 || fail "port $port is already in use by another process."
    elif command -v ss >/dev/null 2>&1; then
      ! ss -ltnH "sport = :$port" | grep -q . || fail "port $port is already in use by another process."
    else
      fail "lsof or ss is required to check host ports."
    fi
  fi
done

if $exists; then
  step "Cluster '$CLUSTER' exists; making sure it is running"
  k3d cluster start $CLUSTER >/dev/null
else
  step "Creating k3d cluster '$CLUSTER' (1 server, no agents, no load balancer, no Traefik)"
  k3d cluster create $CLUSTER --servers 1 --agents 0 --no-lb \
    --k3s-arg "--disable=traefik@server:0" --k3s-arg "--disable=servicelb@server:0" \
    `# Default eviction (<10-15% disk free) taints a typical full laptop disk; use absolute 1 GiB floors.` \
    --k3s-arg "--kubelet-arg=eviction-hard=nodefs.available<1Gi,imagefs.available<1Gi@server:0" \
    `# Keep local disk recovery bounded instead of k3s's default 10% minimum reclaim.` \
    --k3s-arg "--kubelet-arg=eviction-minimum-reclaim=nodefs.available=256Mi,imagefs.available=256Mi@server:0" \
    `# Imported images (imagePullPolicy: Never) cannot be re-pulled, so kubelet must not GC them.` \
    --k3s-arg "--kubelet-arg=image-gc-high-threshold=100@server:0" \
    --k3s-arg "--kubelet-arg=image-gc-low-threshold=99@server:0" \
    -p "8081:30081@server:0:direct" -p "8082:30082@server:0:direct" \
    --kubeconfig-update-default --kubeconfig-switch-context=false --wait >/dev/null
fi

step "Building images (Dockerfile targets, via the Compose build definitions)"
# Compose's builder supports the Dockerfile's BuildKit cache mount even where buildx is missing.
docker compose -p ledgerflow-k8s -f infra/docker-compose.yml build -q payment-service ledger-service
docker tag ledgerflow-k8s-payment-service $PAYMENT_IMAGE
docker tag ledgerflow-k8s-ledger-service $LEDGER_IMAGE

# MongoDB/Kafka images come from the host too (pulled once if missing), so the cluster never pulls.
for img in mongo:8.0 apache/kafka:4.0.0; do
  docker image inspect $img >/dev/null 2>&1 || docker pull -q $img >/dev/null
done
step "Importing images into the cluster"
k3d image import -c $CLUSTER $PAYMENT_IMAGE $LEDGER_IMAGE mongo:8.0 apache/kafka:4.0.0 >/dev/null

wait_for() {
  step "Waiting for $1"
  $K rollout status "$1" --timeout=300s >/dev/null || { $K get pods; fail "$1 did not become ready. Logs: k8s/logs.sh"; }
}

step "Applying MongoDB and Kafka"
kubectl --context $CTX apply -f infra/k8s/namespace.yaml >/dev/null
for f in configmap mongodb kafka; do $K apply -f infra/k8s/$f.yaml >/dev/null; done
wait_for statefulset/mongodb

step "Initializing rs0 if needed, waiting for PRIMARY, and ensuring database-scoped users"
# kubectl exec connects locally, allowing first-run setup through MongoDB's localhost exception.
$K exec -i mongodb-0 -- bash -se <<'MONGO'
if ! mongosh admin --quiet --username local_admin --password local_admin --eval 'quit(db.runCommand({connectionStatus:1}).ok ? 0 : 1)' >/dev/null 2>&1; then
  mongosh admin --quiet --eval "
    try { rs.status(); } catch (error) {
      if (error.code !== 94) throw error;
      rs.initiate({_id:'rs0',members:[{_id:0,host:'mongodb-0.mongodb.ledgerflow.svc.cluster.local:27017'}]});
    }
    for (let attempt = 0; !db.hello().isWritablePrimary; attempt++) {
      if (attempt >= 120) throw new Error('Timed out waiting for MongoDB PRIMARY');
      sleep(1000);
    }
    db.createUser({user:'local_admin',pwd:'local_admin',roles:['root']});
  "
fi
mongosh admin --quiet --username local_admin --password local_admin --eval "
  for (let attempt = 0; !db.hello().isWritablePrimary; attempt++) {
    if (attempt >= 120) throw new Error('Timed out waiting for MongoDB PRIMARY');
    sleep(1000);
  }
  for (const [database,user] of [['payments','payment_user'],['ledger','ledger_user']]) {
    const target = db.getSiblingDB(database);
    if (!target.getUser(user)) target.createUser({user,pwd:user,roles:[{role:'readWrite',db:database}]});
  }
  print('MongoDB rs0 is PRIMARY; database-scoped users are ready');
"
MONGO
wait_for statefulset/kafka

step "Applying payment-service and ledger-service"
for d in payment-service:$PAYMENT_IMAGE ledger-service:$LEDGER_IMAGE; do
  id=$(docker image inspect -f '{{.Id}}' "${d#*:}" | cut -c8-19)
  sed "s|ledgerflow/image-id: .*|ledgerflow/image-id: \"$id\"|" "infra/k8s/${d%%:*}.yaml" | $K apply -f - >/dev/null
done
wait_for deployment/ledger-service
wait_for deployment/payment-service

for url in http://localhost:8081/actuator/health http://localhost:8082/actuator/health; do
  for _ in $(seq 60); do
    curl -fsS --connect-timeout 3 --max-time 5 "$url" 2>/dev/null | grep -q '"status":"UP"' && continue 2
    sleep 1
  done
  fail "$url did not report UP."
done

cat <<READY

LedgerFlow is ready on Kubernetes (k3d cluster '$CLUSTER') in $(( SECONDS - started )) s

Payment API:     http://localhost:8081/api/payments
Payment health:  http://localhost:8081/actuator/health
Ledger health:   http://localhost:8082/actuator/health

Status: k8s/status.sh   Logs: k8s/logs.sh payment|ledger|kafka|mongodb
Smoke check (includes pod recovery): k8s/smoke.sh
Delete the cluster (and its data): k8s/stop.sh
READY
