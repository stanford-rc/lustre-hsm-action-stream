#!/bin/bash
# End-to-end integration test for the hsm-stream-janitor.

set -ex

# --- Test Environment Setup ---
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-2}"

TEST_DIR=$(mktemp -d)
trap 'echo "Cleaning up..."; rm -rf "$TEST_DIR"' EXIT

echo "Using test directory for janitor test: $TEST_DIR"
echo "Using Redis DB: $REDIS_DB on $REDIS_HOST:$REDIS_PORT"

MDT_PATH="$TEST_DIR/sys/kernel/debug/lustre/mdt/testfs-MDT0000"
ACTIONS_FILE="$MDT_PATH/hsm/actions"
SHIPPER_CACHE_FILE="$TEST_DIR/shipper_cache.json"
SHIPPER_CONFIG_FILE="$TEST_DIR/shipper_config.yaml"
JANITOR_CONFIG_FILE="$TEST_DIR/janitor_config.yaml"

mkdir -p "$MDT_PATH/hsm"

# --- Create Configs ---
cat > "$SHIPPER_CONFIG_FILE" <<EOF
mdt_watch_glob: "$ACTIONS_FILE"
cache_path: "$SHIPPER_CACHE_FILE"
poll_interval: 1
redis_host: "$REDIS_HOST"
redis_port: $REDIS_PORT
redis_db: $REDIS_DB
redis_stream_name: "hsm:actions"
log_level: "DEBUG"
EOF

cat > "$JANITOR_CONFIG_FILE" <<EOF
redis_host: "$REDIS_HOST"
redis_port: $REDIS_PORT
redis_db: $REDIS_DB
redis_stream_name: "hsm:actions"
operation_interval_seconds: 1
terminal_state_timeout_seconds: 2 # 2 seconds
log_level: "DEBUG"
EOF

# --- Test Execution ---
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" FLUSHDB

ACTION_A_STARTED="idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED"
ACTION_B_STARTED="idx=[1/2] action=ARCHIVE fid=[0xb] status=STARTED"
ACTION_C_STARTED="idx=[1/3] action=ARCHIVE fid=[0xc] status=WAITING"
ACTION_B_SUCCEED="idx=[1/2] action=ARCHIVE fid=[0xb] status=SUCCEED"

# 1. Populate Redis with initial state
echo "--- TEST 1: Populating Redis stream ---"
echo "$ACTION_A_STARTED" > "$ACTIONS_FILE"
echo "$ACTION_B_STARTED" >> "$ACTIONS_FILE"
echo "$ACTION_C_STARTED" >> "$ACTIONS_FILE"
PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$SHIPPER_CONFIG_FILE" --run-once

# 2. Create a terminal state (B=SUCCEED) and a purged state (A is removed)
echo "--- TEST 2: Creating terminal and purged states ---"
echo "$ACTION_B_SUCCEED" > "$ACTIONS_FILE"
echo "$ACTION_C_STARTED" >> "$ACTIONS_FILE"
PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$SHIPPER_CONFIG_FILE" --run-once

# 3. Run the janitor once to establish its baseline 'last_seen' timestamps.
echo "--- TEST 3: Initial janitor run to establish state ---"
PYTHONPATH=src python -m lustre_hsm_action_stream.janitor -c "$JANITOR_CONFIG_FILE" --run-once

# 4. Wait for the stale timeout to pass.
echo "Waiting for stale timeout to pass..."
sleep 3

# 5. Run the janitor a SECOND time. This is the actual test run.
echo "--- TEST 4: Running janitor again to reap stale action ---"
# This run will see that action B's 'last_seen' timestamp is ~3 seconds ago,
# which is greater than the 2-second timeout, triggering the reap.
JANITOR_LOG=$(PYTHONPATH=src python -m lustre_hsm_action_stream.janitor -c "$JANITOR_CONFIG_FILE" --run-once 2>&1)

# 6. Verify the results.
echo "--- TEST 5: Verifying results ---"
# Check the log for the "Reaping" message, confirming the timeout logic worked.
# This is a much cleaner way to check the log content.
echo "$JANITOR_LOG" | grep "Reaping stale terminal action"

# Check the Redis stream length. It should be small, as events for A and B are now obsolete.
STREAM_LEN=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" XLEN hsm:actions)
if [ "$STREAM_LEN" -gt 2 ]; then
    echo "ERROR: Stream was not trimmed correctly! Length is $STREAM_LEN"
    exit 1
fi

echo "✅ Janitor test passed successfully!"
