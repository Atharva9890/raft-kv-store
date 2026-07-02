#!/usr/bin/env bash
# Finds whichever node currently prints "role=LEADER" in its logs and
# kills that container, so you can watch the rest of the cluster
# re-elect a new leader in `docker compose logs -f`.
#
# Only meaningful once the election.go/log.go TODOs are implemented -
# on the unmodified scaffold no node ever becomes leader, so this
# script will report "no leader found" instead.
set -euo pipefail

leader=""
for svc in node1 node2 node3 node4 node5; do
  if docker compose logs --tail 5 "$svc" 2>/dev/null | grep -q "role=LEADER"; then
    leader="$svc"
  fi
done

if [[ -z "$leader" ]]; then
  echo "no leader found (have you implemented the election.go TODOs yet?)" >&2
  exit 1
fi

echo "killing $leader ..."
docker compose kill "$leader"
echo "watch the remaining nodes elect a new leader with: docker compose logs -f"
