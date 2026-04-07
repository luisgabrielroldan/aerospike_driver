#!/bin/sh
set -e

CONFIG_FILE="${1:-/opt/aerospike/aerospike.conf}"

# Start Aerospike in the background
/usr/bin/asd --config-file "$CONFIG_FILE" --foreground &
ASD_PID=$!

# Wait for the service port to accept connections
echo "Waiting for Aerospike to start..."
for i in $(seq 1 30); do
  if asinfo -v status 2>/dev/null | grep -q ok; then
    break
  fi
  sleep 1
done

# Auto-set roster for SC namespaces so the partition map is valid immediately
NODE_ID=$(asinfo -v 'node' 2>/dev/null | tr -d '[:space:]')
if [ -n "$NODE_ID" ]; then
  for NS in $(asinfo -v 'namespaces' 2>/dev/null | tr ';' '\n'); do
    SC=$(asinfo -v "get-config:context=namespace;id=$NS" 2>/dev/null | tr ';' '\n' | grep '^strong-consistency=' | cut -d= -f2)
    if [ "$SC" = "true" ]; then
      CURRENT=$(asinfo -v "roster:namespace=$NS" 2>/dev/null | grep 'roster=null' || true)
      if [ -n "$CURRENT" ]; then
        echo "Setting roster for SC namespace '$NS' -> $NODE_ID"
        asinfo -v "roster-set:namespace=$NS;nodes=$NODE_ID"
      fi
    fi
  done
  asinfo -v 'recluster:' 2>/dev/null
fi

# Foreground the server process
wait $ASD_PID
