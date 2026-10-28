#!/usr/bin/env bash
# Six delivery demonstrations in a fresh project; all containers and data are preserved.
set -euo pipefail
exec bash "$(dirname "$0")/demo.sh" "$@"
