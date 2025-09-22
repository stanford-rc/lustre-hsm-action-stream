#!/bin/bash
# Smoke test for the hsm-action-top viewer.

set -ex

# --- Test Environment Setup ---
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-2}" # Use the same test DB

TEST_DIR=$(mktemp -d)
trap 'echo "Cleaning up..."; rm -rf "$TEST_DIR"' EXIT

echo "Using test directory for viewer test: $TEST_DIR"
echo "Using Redis DB: $REDIS_DB on $REDIS_HOST:$REDIS_PORT"

MDT_PATH="$TEST_DIR/sys/kernel/debug/lustre/mdt/testfs-MDT0000"
ACTIONS_FILE="$MDT_PATH/hsm/actions"
CACHE_FILE="$TEST_DIR/cache.json"
CONFIG_FILE="$TEST_DIR/config.yaml"

# --- Test Execution ---

# 1. Create test data in Redis using the shipper.
# This ensures we have a realistic stream for the viewer to consume.
echo "--- PREP: Populating Redis with test data ---"

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
ACTION_1_STARTED="idx=[1/1] action=ARCHIVE fid=[0x1] status=STARTED"
echo "$ACTION_1_STARTED" > "$ACTIONS_FILE"

# Clean DB and run shipper once to populate it.
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" FLUSHDB
PYTHONPATH=src python -m lustre_hsm_action_stream.shipper -c "$CONFIG_FILE" --run-once


# 2. Run the viewer smoke test.
# The test passes if the viewer can start, consume the stream, draw one frame,
# and exit cleanly (exit code 0) without crashing.
echo "--- TEST: Running viewer in run-once mode ---"

PYTHONPATH=src python -m lustre_hsm_action_stream.viewer \
    --host "$REDIS_HOST" \
    --port "$REDIS_PORT" \
    --db "$REDIS_DB" \
    --run-once

echo "✅ Viewer smoke test passed successfully!"
