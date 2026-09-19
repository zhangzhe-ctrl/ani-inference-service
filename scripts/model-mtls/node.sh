#!/usr/bin/env bash
# Run on Fedora. Reuse the authorized cluster access without logging credentials.
set -euo pipefail
node=${1:?node IP}; shift
case "$node" in 172.16.101.10|172.16.101.11|172.16.101.12) ;; *) exit 2;; esac
export SSH_ASKPASS=/home/chabking/ani-installer-runs/platform-20260918/access/askpass.sh
export SSH_ASKPASS_REQUIRE=force
exec setsid -w ssh -o StrictHostKeyChecking=accept-new -o NumberOfPasswordPrompts=1 -o BatchMode=no -o ConnectTimeout=10 ubuntu@"$node" "$@"
