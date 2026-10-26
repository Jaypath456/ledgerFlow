#!/usr/bin/env bash
set -euo pipefail

# On an empty volume MongoDB's localhost exception permits replica-set and root setup.
if ! mongosh admin --quiet --username local_admin --password local_admin --eval 'quit(db.runCommand({connectionStatus:1}).ok ? 0 : 1)' >/dev/null 2>&1; then
  mongosh admin --quiet --eval "
    try { rs.status(); } catch (error) {
      if (error.code !== 94) throw error;
      rs.initiate({_id:'rs0',members:[{_id:0,host:'mongo:27017'}]});
    }
    if (!db.hello().isWritablePrimary) quit(1);
    db.createUser({user:'local_admin',pwd:'local_admin',roles:['root']});
  "
fi
exec mongosh admin --quiet --username local_admin --password local_admin --eval "
  if (!db.hello().isWritablePrimary) quit(1);
  for (const [database,user] of [['payments','payment_user'],['ledger','ledger_user']]) {
    const target = db.getSiblingDB(database);
    if (!target.getUser(user)) target.createUser({user,pwd:user,roles:[{role:'readWrite',db:database}]});
  }
"
