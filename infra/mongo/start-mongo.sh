#!/usr/bin/env bash
set -euo pipefail
rm -f /tmp/ledgerflow-ready
printf '%s' ledgerflowLocalReplicaSetKey > /data/db/replica-key
chmod 400 /data/db/replica-key
mongod --replSet rs0 --bind_ip_all --keyFile /data/db/replica-key &
mongo_pid=$!
trap 'kill -TERM "$mongo_pid"; wait "$mongo_pid"' TERM INT
until mongosh --quiet --eval 'quit(db.adminCommand({ping:1}).ok ? 0 : 1)' >/dev/null 2>&1; do
  kill -0 "$mongo_pid"
  sleep 1
done
if ! mongosh admin --quiet --username local_admin --password local_admin --eval 'quit(db.runCommand({connectionStatus:1}).ok ? 0 : 1)' >/dev/null 2>&1; then
  mongosh admin --quiet --eval "
    rs.initiate({_id:'rs0',members:[{_id:0,host:'mongo:27017'}]});
    while (!db.hello().isWritablePrimary) sleep(100);
    db.createUser({user:'local_admin',pwd:'local_admin',roles:['root']});
  "
fi
mongosh admin --quiet --username local_admin --password local_admin --eval "
  while (!db.hello().isWritablePrimary) sleep(100);
  for (const [database,user] of [['payments','payment_user'],['ledger','ledger_user']]) {
    const target = db.getSiblingDB(database);
    if (!target.getUser(user)) target.createUser({user,pwd:user,roles:[{role:'readWrite',db:database}]});
  }
"
touch /tmp/ledgerflow-ready
wait "$mongo_pid"
