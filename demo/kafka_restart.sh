#!/usr/bin/env bash
# Restart the Kafka broker under traffic. Outbox relays keep unacknowledged rows and resend;
# consumers dedupe; nothing is lost or doubled.
source "$(dirname "$0")/lib.sh"; require_stack
restart_kafka() { $C restart kafka >/dev/null 2>&1; }
infra_demo "Kafka restart" "restart the Kafka broker" restart_kafka
