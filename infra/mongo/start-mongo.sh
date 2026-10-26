#!/usr/bin/env bash
set -euo pipefail
printf '%s' ledgerflowLocalReplicaSetKey > /data/db/replica-key
chmod 400 /data/db/replica-key
exec mongod --replSet rs0 --bind_ip_all --keyFile /data/db/replica-key
