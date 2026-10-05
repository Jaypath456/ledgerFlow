#!/usr/bin/env bash
# Freeze Postgres for 10 s under traffic (docker pause). Requests stall rather than fail
# half-way; afterwards everything drains and invariants hold.
source "$(dirname "$0")/lib.sh"; require_stack
pause_pg() { $C pause postgres >/dev/null 2>&1; sleep 10; $C unpause postgres >/dev/null 2>&1; }
infra_demo "Postgres pause" "docker pause postgres for 10 s" pause_pg
