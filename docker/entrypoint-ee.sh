#!/bin/sh
set -e

CONFIG_FILE="${1:-/opt/aerospike/aerospike.conf}"

/usr/bin/asd --config-file "$CONFIG_FILE" --foreground &
ASD_PID=$!

echo "Waiting for Aerospike to start..."
for i in $(seq 1 60); do
  if asinfo -v status 2>/dev/null | grep -q ok; then
    echo "Aerospike is up after ${i}s"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "Aerospike did not start within 60s"
  fi
  sleep 1
done

NODE_ID=$(asinfo -v 'node' 2>/dev/null | tr -d '[:space:]')
if [ -z "$NODE_ID" ]; then
  echo "Could not get node ID, skipping roster setup"
  wait $ASD_PID
fi

for NS in $(asinfo -v 'namespaces' 2>/dev/null | tr ';' '\n'); do
  SC=$(asinfo -v "get-config:context=namespace;id=$NS" 2>/dev/null \
    | tr ';' '\n' | grep '^strong-consistency=' | cut -d= -f2)
  [ "$SC" = "true" ] || continue

  echo "Waiting for SC namespace '$NS' to be queryable..."
  for attempt in $(seq 1 30); do
    ROSTER_OUT=$(asinfo -v "roster:namespace=$NS" 2>/dev/null || true)
    if echo "$ROSTER_OUT" | grep -q 'roster='; then
      break
    fi
    if [ "$attempt" -eq 30 ]; then
      echo "Namespace '$NS' not queryable after 30s, skipping roster setup"
      continue 2
    fi
    sleep 1
  done

  if echo "$ROSTER_OUT" | grep -q 'roster=null'; then
    echo "Setting roster for SC namespace '$NS' -> $NODE_ID"
    asinfo -v "roster-set:namespace=$NS;nodes=$NODE_ID"
  else
    echo "Roster for '$NS' already set, skipping"
  fi
done

asinfo -v 'recluster:' 2>/dev/null || true

wait $ASD_PID
