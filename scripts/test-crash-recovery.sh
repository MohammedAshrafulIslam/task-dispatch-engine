#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

WORKER_CMD=${WORKER_CMD:-"node --loader ts-node/esm src/tracker.ts"}
TASK_ID=${1:-CRASH-001}
LEASE_WAIT_SECONDS=${LEASE_WAIT_SECONDS:-70}

has_processing_log() {
  if command -v rg >/dev/null 2>&1; then
    rg -q "Processing order ${TASK_ID}" test-crash-worker1.log
  else
    grep -q "Processing order ${TASK_ID}" test-crash-worker1.log
  fi
}

echo "[1/6] Starting worker..."
($WORKER_CMD 2>&1 | tee test-crash-worker1.log) &
W1_PID=$!
sleep 5

echo "[2/6] Sending message ${TASK_ID}..."
CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")
printf '{"order_id":"%s","user":"Ashraf","item":"Macbook","quantity":1,"createdAt":"%s"}\n' \
  "$TASK_ID" "$CREATED_AT" | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic orders

echo "[3/6] Waiting for processing to start..."
found=0
for _ in $(seq 1 30); do
  if has_processing_log; then
    found=1
    break
  fi
  sleep 1
done

if [ "$found" -ne 1 ]; then
  echo "Did not see processing start for ${TASK_ID}. Stopping worker."
  kill "$W1_PID" 2>/dev/null || true
  exit 1
fi

echo "[4/6] Crashing worker..."
kill -9 "$W1_PID" 2>/dev/null || true
sleep 1

echo "[5/6] Waiting for lease expiry (${LEASE_WAIT_SECONDS}s)..."
sleep "$LEASE_WAIT_SECONDS"

echo "[6/6] Restarting worker..."
($WORKER_CMD 2>&1 | tee test-crash-worker2.log) &
W2_PID=$!
sleep 20
kill "$W2_PID" 2>/dev/null || true
wait "$W2_PID" 2>/dev/null || true

echo "DB check for ${TASK_ID}:"
docker exec postgres psql -U ashraf -d taskengine -c \
"select task_id, status, retries, worker_id from task_outcomes where task_id='${TASK_ID}';"

echo "Processing rows:"
docker exec postgres psql -U ashraf -d taskengine -c \
"select count(*) as processing_count from task_outcomes where status='PROCESSING';"

echo "Duplicate rows:"
docker exec postgres psql -U ashraf -d taskengine -c \
"select count(*) as duplicate_rows from (select task_id from task_outcomes group by task_id having count(*) > 1) d;"
