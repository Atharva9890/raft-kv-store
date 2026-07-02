#!/usr/bin/env bash
# finds whichever node is currently printing "role=LEADER" and kills
# that container, so I can watch the rest of the cluster re-elect in
# `docker compose logs -f`. since I'm running with a real
# FilePersister and a mounted volume per node now, `docker compose
# start <that node>` afterwards brings it back with its term/vote/log
# intact instead of starting from a blank slate.
set -euo pipefail

leader=""
for svc in node1 node2 node3 node4 node5; do
  if docker compose logs --tail 5 "$svc" 2>/dev/null | grep -q "role=LEADER"; then
    leader="$svc"
  fi
done

if [[ -z "$leader" ]]; then
  echo "no leader found in the last few log lines - give the cluster a couple more seconds and try again" >&2
  exit 1
fi

echo "killing $leader ..."
docker compose kill "$leader"
echo "watch the remaining nodes elect a new leader with: docker compose logs -f"
echo "bring it back (with its state intact) with: docker compose start $leader"
