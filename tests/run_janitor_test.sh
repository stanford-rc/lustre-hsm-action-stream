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
# Shipper config is unchanged, but heartbeat interval is not used in this test.
cat > "$SHIPPER_CONFIG_FILE" <<EOF
mdt_watch_glob: "$ACTIONS_FILE"
cache_path: "$SHIPPER_CACHE_FILE"
poll_interval: 1
redis_host: "$REDIS_HOST"
redis_port: $REDIS_PORT
redis_db: $REDIS_DB
redis_stream_name: "hsm:actions"
heartbeat_interval: 3600
log_level: "DEBUG"
EOF

# Use xdel_reaped: true in the test config to make verification easier
cat > "$JANITOR_CONFIG_FILE" <<EOF
redis_host: "$REDIS_HOST"
redis_port: $REDIS_PORT
redis_db: $REDIS_DB
redis_stream_name: "hsm:actions"
operation_interval_seconds: 1
stale_action_timeout_seconds: 2
xdel_reaped: true
log_level: "DEBUG"
EOF

# --- Test Execution ---
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" FLUSHDB

ACTION_A_STARTED="idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED"
ACTION_B_STARTED="idx=[1/2] action=ARCHIVE fid=[0xb] status=STARTED"
ACTION_C_WAITING="idx=[1/3] action=ARCHIVE fid=[0xc] status=WAITING"
ACTION_B_SUCCEED="idx=[1/2] action=ARCHIVE fid=[0xb] status=SUCCEED"

# 1. Populate stream with 3 NEW events.
echo "--- TEST 1: Populating Redis stream ---"
echo "$ACTION_A_STARTED" > "$ACTIONS_FILE"
echo "$ACTION_B_STARTED" >> "$ACTIONS_FILE"
echo "$ACTION_C_WAITING" >> "$ACTIONS_FILE"
PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$SHIPPER_CONFIG_FILE" --run-once

# 2. Create a purged state (A is removed) and a terminal state (B -> SUCCEED).
echo "--- TEST 2: Creating terminal and purged states ---"
echo "$ACTION_B_SUCCEED" > "$ACTIONS_FILE"
echo "$ACTION_C_WAITING" >> "$ACTIONS_FILE" # C is unchanged
PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$SHIPPER_CONFIG_FILE" --run-once

# Stream now has 5 events: NEW A, NEW B, NEW C, PURGED A, UPDATE B

# 3. Establish initial janitor state. It will see the SUCCEED state for B.
echo "--- TEST 3: Initial janitor run to establish state ---"
PYTHONPATH=src python -m lustre_hsm_action_stream.janitor -c "$JANITOR_CONFIG_FILE" --run-once

# 4. Wait for the stale timeout to pass.
echo "Waiting for stale timeout to pass..."
sleep 3

# 5. Run the janitor again. This is the main test.
echo "--- TEST 4: Running janitor again to reap and trim ---"
JANITOR_LOG=$(PYTHONPATH=src python -m lustre_hsm_action_stream.janitor -c "$JANITOR_CONFIG_FILE" --run-once 2>&1)

# 6. Verify the results.
echo "--- TEST 5: Verifying results ---"
echo "$JANITOR_LOG"

# Check that the reaping message was logged for both actions.
# This proves the new logic is working.
if [ $(echo "$JANITOR_LOG" | grep -c "Reaping stale action") -ne 2 ]; then
    echo "ERROR: Expected to reap 2 stale actions, but didn't find the log messages."
    exit 1
fi

# The final stream length should be exactly 1.
# The remaining event is `PURGED A`.
# `NEW A` and `NEW B` were trimmed in step 3.
# `UPDATE B` and `NEW C` were reaped and XDEL'd in step 5.
STREAM_LEN=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" XLEN hsm:actions)
if [ "$STREAM_LEN" -ne 1 ]; then
    echo "ERROR: Stream was not trimmed correctly! Expected length 1, but got $STREAM_LEN"
    exit 1
fi

echo "✅ Janitor test passed successfully!"
