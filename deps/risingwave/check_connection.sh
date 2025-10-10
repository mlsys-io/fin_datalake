#!/bin/bash
set -euo pipefail

# === Configuration ===
NAMESPACE="risingwave"
SERVICE_NAME="risingwave"
LOCAL_PORT=4567
REMOTE_PORT=4567
DB_NAME="dev"
USER="root"
QUERY="SELECT 1;"
WAIT_SECONDS=5   # wait for port-forward to establish

# === Start port-forward in background ===
echo "[INFO] Starting temporary port-forward..."
kubectl -n "$NAMESPACE" port-forward svc/"$SERVICE_NAME" "$LOCAL_PORT":"$REMOTE_PORT" > /dev/null 2>&1 &
PF_PID=$!

# Ensure port-forward is cleaned up if script exits early
trap "kill $PF_PID" EXIT

# Wait a few seconds for port-forward to be ready
sleep "$WAIT_SECONDS"

# === Run sanity check query ===
echo "[INFO] Running sanity check query..."
psql -h localhost -p "$LOCAL_PORT" -U "$USER" -d "$DB_NAME" -c "$QUERY"


# === Kill port-forward (handled automatically by trap) ===
echo "[INFO] Port-forward stopped."
