# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-reconciler

A utility to validate the Redis HSM event stream against the live state
of Lustre's hsm/actions logs.
"""

import sys
import glob
import json
import os.path
import redis
import argparse

# Import the single, shared parser function.
from .parser import parse_action_line

def get_ground_truth_state(mdt_glob):
    """Scans hsm/actions files for live state, keyed by (MDT, cat_idx, rec_idx)."""
    print(f"Reading ground truth from {mdt_glob}...")
    ground_truth = {}
    for action_file in glob.glob(mdt_glob):
        mdt_name = os.path.basename(os.path.dirname(os.path.dirname(action_file)))
        try:
            with open(action_file, 'r') as f:
                for line in f:
                    data = parse_action_line(line) # Now calls the imported function
                    if not data:
                        continue
                    key = (mdt_name, data['cat_idx'], data['rec_idx'])
                    ground_truth[key] = data.get('status')
        except Exception as e:
            print(f"  - WARNING: Could not read {action_file}: {e}", file=sys.stderr)
    print(f"Found {len(ground_truth)} total live actions in local hsm/actions files.")
    return ground_truth

def get_stream_derived_state(r, stream_name):
    """Scans Redis stream from the beginning to reconstruct the latest state for each action."""
    print(f"Replaying global Redis stream '{stream_name}' from beginning...")
    stream_state = {}
    last_id = '0-0'
    total_events = 0
    while True:
        # Reading in large chunks is more efficient
        response = r.xread({stream_name: last_id}, count=50000)
        if not response:
            break
        for _, messages in response:
            for msg_id, event_data in messages:
                total_events += 1
                try:
                    event = json.loads(event_data[b'data'])
                    key = (event['mdt'], event['cat_idx'], event['rec_idx'])
                    if event['event_type'] in ("NEW", "UPDATE"):
                        stream_state[key] = event.get('status')
                    elif event['event_type'] == "PURGED":
                        stream_state.pop(key, None)
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"  - WARNING: Could not parse event {msg_id}: {e}", file=sys.stderr)
                last_id = msg_id
    print(f"Processed {total_events} events. Derived global state has {len(stream_state)} live actions.")
    return stream_state

def reconcile(mdt_glob, redis_args, scope_mdts):
    """Main function to connect, fetch states, and compare them."""
    try:
        r = redis.Redis(
            host=redis_args['host'], port=redis_args['port'], db=redis_args['db'],
            decode_responses=False, socket_connect_timeout=5, socket_timeout=15
        )
        r.ping()
        print(f"Connected to Redis at {redis_args['host']}:{redis_args['port']}.")
    except Exception as e:
        print(f"FATAL: Could not connect to Redis: {e}", file=sys.stderr)
        return False

    full_ground_truth = get_ground_truth_state(mdt_glob)
    full_stream_state = get_stream_derived_state(r, redis_args['stream_name'])

    ground_truth = full_ground_truth
    stream_state = full_stream_state

    # Scope the comparison if MDTs are specified
    if scope_mdts:
        print(f"\nScoping validation to MDTs: {', '.join(scope_mdts)}")
        scope_mdts = set(scope_mdts)
        ground_truth = {k: v for k, v in full_ground_truth.items() if k[0] in scope_mdts}
        stream_state = {k: v for k, v in full_stream_state.items() if k[0] in scope_mdts}
        print(f"Scoped ground truth has {len(ground_truth)} actions.")
        print(f"Scoped stream state has {len(stream_state)} actions.")

    truth_keys = set(ground_truth.keys())
    stream_keys = set(stream_state.keys())

    missing_from_stream = truth_keys - stream_keys
    extra_in_stream = stream_keys - truth_keys
    mismatched_status = [
        (key, f"hsm/actions='{ground_truth.get(key)}'", f"stream='{stream_state.get(key)}'")
        for key in truth_keys.intersection(stream_keys)
        if ground_truth.get(key) != stream_state.get(key)
    ]

    print("\n--- Reconciliation Report ---")
    is_valid = True
    if missing_from_stream:
        print(f"\nERROR: {len(missing_from_stream)} actions found in hsm/actions but MISSING from stream state:")
        for key in sorted(list(missing_from_stream))[:20]:
            print(f"  - Key: {key}, Status in hsm/actions: '{ground_truth.get(key)}'")
        is_valid = False

    if extra_in_stream:
        print(f"\nERROR: {len(extra_in_stream)} actions found in stream state but are PURGED (or from unscoped MDT):")
        for key in sorted(list(extra_in_stream))[:20]:
            print(f"  - Key: {key}, Status in stream: '{stream_state.get(key)}'")
        is_valid = False

    if mismatched_status:
        print(f"\nNOTE: {len(mismatched_status)} actions have MISMATCHED statuses (likely benign race condition):")
        for key, truth, stream in mismatched_status[:20]:
            print(f"  - {key}: {truth}, {stream}")

    print("-" * 30)
    if is_valid:
        print("\nSUCCESS: Validation complete for the specified scope. No critical discrepancies found.")
        return True
    else:
        print("\nFAILURE: Critical discrepancies found. Review the errors above.")
        return False

def main():
    """Main entry point for the hsm-stream-reconciler executable."""
    parser = argparse.ArgumentParser(description="Validate HSM Redis stream against Lustre's hsm/actions files.")
    parser.add_argument('--glob', default="/sys/kernel/debug/lustre/mdt/*-MDT*/hsm/actions", help="Glob path to hsm/actions files.")
    parser.add_argument('--host', default="localhost", help="Redis server host.")
    parser.add_argument('--port', type=int, default=6379, help="Redis server port.")
    parser.add_argument('--db', type=int, default=1, help="Redis database number.")
    parser.add_argument('--stream', default="hsm:actions", help="Name of the Redis Stream.")
    parser.add_argument('--mdts', nargs='+', help="Optional: Space-separated list of MDT names to validate (e.g., elm-MDT0001).")
    args = parser.parse_args()

    redis_conn_args = {
        'host': args.host,
        'port': args.port,
        'db': args.db,
        'stream_name': args.stream
    }

    if not reconcile(args.glob, redis_conn_args, args.mdts):
        sys.exit(1)

if __name__ == "__main__":
    main()
