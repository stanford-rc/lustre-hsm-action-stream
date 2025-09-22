#!/bin/bash
# End-to-end integration test for the shipper and reconciler.

set -ex

# --- Test Environment Setup ---
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-2}"

TEST_DIR=$(mktemp -d)
trap 'echo "Cleaning up..."; rm -rf "$TEST_DIR"' EXIT

echo "Using test directory: $TEST_DIR"
echo "Using Redis DB: $REDIS_DB on $REDIS_HOST:$REDIS_PORT"

MDT_PATH="$TEST_DIR/sys/kernel/debug/lustre/mdt/testfs-MDT0000"
ACTIONS_FILE="$MDT_PATH/hsm/actions"
CACHE_FILE="$TEST_DIR/cache.json"
CONFIG_FILE="$TEST_DIR/config.yaml"

cat > "$CONFIG_FILE" <<EOF
mdt_watch_glob: "$ACTIONS_FILE"
cache_path: "$CACHE_FILE"
poll_interval: 1
redis_host: "$REDIS_HOST"
redis_port: $REDIS_PORT
redis_db: $REDIS_DB
redis_stream_name: "hsm:actions"
log_level: "DEBUG"
EOF

mkdir -p "$MDT_PATH/hsm"

# --- Test Execution ---

ACTION_1_STARTED="idx=[1/1] action=ARCHIVE fid=[0x1] status=STARTED"
ACTION_2_STARTED="idx=[1/2] action=ARCHIVE fid=[0x2] status=STARTED"
ACTION_1_WAITING="idx=[1/1] action=ARCHIVE fid=[0x1] status=WAITING"

redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" FLUSHDB

# 1. Initial State: Two new actions appear.
echo "--- TEST 1: Initial 'NEW' event detection ---"
echo "$ACTION_1_STARTED" > "$ACTIONS_FILE"
echo "$ACTION_2_STARTED" >> "$ACTIONS_FILE"

PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$CONFIG_FILE" --run-once

PYTHONPATH=src python -m lustre_hsm_action_stream.reconciler \
  --glob "$ACTIONS_FILE" --host "$REDIS_HOST" --port "$REDIS_PORT" --db "$REDIS_DB"


# 2. Update State: One action's status changes.
echo "--- TEST 2: 'UPDATE' event detection ---"
echo "$ACTION_1_WAITING" > "$ACTIONS_FILE"
echo "$ACTION_2_STARTED" >> "$ACTIONS_FILE"

PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$CONFIG_FILE" --run-once

PYTHONPATH=src python -m lustre_hsm_action_stream.reconciler \
  --glob "$ACTIONS_FILE" --host "$REDIS_HOST" --port "$REDIS_PORT" --db "$REDIS_DB"


# 3. Purge State: One action is completed and removed.
echo "--- TEST 3: 'PURGED' event detection ---"
echo "$ACTION_2_STARTED" > "$ACTIONS_FILE"

PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$CONFIG_FILE" --run-once

PYTHONPATH=src python -m lustre_hsm_action_stream.reconciler \
  --glob "$ACTIONS_FILE" --host "$REDIS_HOST" --port "$REDIS_PORT" --db "$REDIS_DB"


# 4. Final State: All actions are gone.
echo "--- TEST 4: Final 'PURGED' event detection ---"
> "$ACTIONS_FILE"

PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$CONFIG_FILE" --run-once

PYTHONPATH=src python -m lustre_hsm_action_stream.reconciler \
  --glob "$ACTIONS_FILE" --host "$REDIS_HOST" --port "$REDIS_PORT" --db "$REDIS_DB"

echo "✅ All end-to-end tests passed successfully!"
