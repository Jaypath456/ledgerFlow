#!/usr/bin/env bash
# kill -9 ledger-service under traffic, restart it after 3 s. Unacknowledged Kafka messages are
# redelivered and deduped; every payment still settles exactly once.
source "$(dirname "$0")/lib.sh"; require_stack
crash() { $C kill ledger-service >/dev/null 2>&1; sleep 3; $C start ledger-service >/dev/null 2>&1; }
infra_demo "Ledger crash recovery" "kill -9 ledger-service, restart after 3 s" crash
