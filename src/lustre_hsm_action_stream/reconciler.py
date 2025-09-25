# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-reconciler

A utility to validate the Redis HSM event streams against the live state
of Lustre's hsm/actions logs.
"""

import sys
import glob
import os.path
import argparse
import logging

# Import the shared parser and the new consumer API
from .parser import parse_action_line
from .consumer import StreamReader

def get_ground_truth_state(mdt_glob):
    """Scans hsm/actions files for live state, keyed by (MDT, cat_idx, rec_idx)."""
    logging.info(f"Reading ground truth from Lustre filesystem at: {mdt_glob}...")
    ground_truth = {}
    for action_file in glob.glob(mdt_glob):
        mdt_name = os.path.basename(os.path.dirname(os.path.dirname(action_file)))
        try:
            with open(action_file, 'r') as f:
                for line in f:
                    data = parse_action_line(line)
                    if not data:
                        continue
                    key = (mdt_name, data['cat_idx'], data['rec_idx'])
                    ground_truth[key] = data.get('status')
        except Exception as e:
            logging.error(f"  - WARNING: Could not read {action_file}: {e}")
    logging.info(f"Found {len(ground_truth)} total live actions in local hsm/actions files.")
    return ground_truth

def get_stream_derived_state(reader):
    """
    Uses a StreamReader to replay all discovered streams and build an
    in-memory model of the live actions.
    """
    logging.info(f"Replaying all streams with prefix '{reader.prefix}:*'...")
    stream_state = {}
    events_processed = 0

    # Use a short block_ms to know when the historical replay is complete.
    for event in reader.events(from_beginning=True, block_ms=200):
        if not event:
            # No more historical events, replay is done.
            break

        events_processed += 1
        e_data = event.data
        try:
            key = (e_data['mdt'], e_data['cat_idx'], e_data['rec_idx'])
            if e_data['event_type'] in ("NEW", "UPDATE"):
                stream_state[key] = e_data.get('status')
            elif e_data['event_type'] == "PURGED":
                stream_state.pop(key, None)
        except (KeyError, TypeError) as e:
            logging.warning(f"Could not parse event {event.id} in {event.stream}, skipping. Error: {e}")

    logging.info(f"Processed {events_processed} events. Derived Redis state has {len(stream_state)} live actions.")
    return stream_state

def reconcile(mdt_glob, redis_args, scope_mdts):
    """Main function to connect, fetch states, and compare them."""
    try:
        reader = StreamReader(
            host=redis_args['host'],
            port=redis_args['port'],
            db=redis_args['db'],
            prefix=redis_args['prefix']
        )
        # The reader connects on first use, we can discover streams to check connection.
        if not reader.discover_streams():
             # If no streams exist yet, the states are vacuously consistent.
            logging.warning("No Redis streams found. Assuming consistent state (0 actions).")
    except Exception as e:
        logging.critical(f"FATAL: Could not configure StreamReader: {e}")
        return False

    full_ground_truth = get_ground_truth_state(mdt_glob)
    full_stream_state = get_stream_derived_state(reader)

    ground_truth = full_ground_truth
    stream_state = full_stream_state

    # Scope the comparison if MDTs are specified
    if scope_mdts:
        print(f"\nScoping validation to MDTs: {', '.join(scope_mdts)}")
        scope_mdts_set = set(scope_mdts)
        ground_truth = {k: v for k, v in full_ground_truth.items() if k[0] in scope_mdts_set}
        stream_state = {k: v for k, v in full_stream_state.items() if k[0] in scope_mdts_set}
        print(f"Scoped ground truth has {len(ground_truth)} actions.")
        print(f"Scoped stream state has {len(stream_state)} actions.")

    truth_keys = set(ground_truth.keys())
    stream_keys = set(stream_state.keys())

    missing_from_stream = truth_keys - stream_keys
    extra_in_stream = stream_keys - truth_keys
    mismatched_status = {
        key: (ground_truth[key], stream_state[key])
        for key in truth_keys.intersection(stream_keys)
        if ground_truth[key] != stream_state[key]
    }

    print("\n--- Reconciliation Report ---")
    is_valid = True
    if missing_from_stream:
        is_valid = False
        print(f"\nERROR: {len(missing_from_stream)} actions found in hsm/actions but MISSING from stream state:")
        for key in sorted(list(missing_from_stream))[:20]:
            print(f"  - Key: {key}, Status in hsm/actions: '{ground_truth.get(key)}'")
        if len(missing_from_stream) > 20: print("  - ... and more.")

    if extra_in_stream:
        is_valid = False
        print(f"\nERROR: {len(extra_in_stream)} actions found in stream state but are PURGED from filesystem:")
        for key in sorted(list(extra_in_stream))[:20]:
            print(f"  - Key: {key}, Status in stream: '{stream_state.get(key)}'")
        if len(extra_in_stream) > 20: print("  - ... and more.")

    if mismatched_status:
        # This is often not a critical error, just a note about race conditions.
        print(f"\nNOTE: {len(mismatched_status)} actions have MISMATCHED statuses (likely benign race condition):")
        for key, (truth_status, stream_status) in list(mismatched_status.items())[:20]:
            print(f"  - {key}: hsm/actions='{truth_status}', stream='{stream_status}'")
        if len(mismatched_status) > 20: print("  - ... and more.")

    print("-" * 30)
    if is_valid:
        print("\nSUCCESS: Validation complete. The stream state is consistent with the filesystem ground truth for the specified scope.")
        return True
    else:
        print("\nFAILURE: Critical discrepancies found. Review the errors above.")
        return False

def main():
    """Main entry point for the hsm-stream-reconciler executable."""
    logging.basicConfig(level="INFO", format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Validate HSM Redis streams against Lustre's hsm/actions files.")
    parser.add_argument('--glob', default="/sys/kernel/debug/lustre/mdt/*-MDT*/hsm/actions", help="Glob path to hsm/actions files.")
    parser.add_argument('--host', default="localhost", help="Redis server host.")
    parser.add_argument('--port', type=int, default=6379, help="Redis server port.")
    parser.add_argument('--db', type=int, default=1, help="Redis database number.")
    parser.add_argument('--stream-prefix', default="hsm:actions", help="Prefix of Redis Streams to discover.")
    parser.add_argument('--mdts', nargs='+', help="Optional: Space-separated list of MDT names to validate (e.g., elm-MDT0001).")
    args = parser.parse_args()

    redis_conn_args = {
        'host': args.host,
        'port': args.port,
        'db': args.db,
        'prefix': args.stream_prefix,
    }

    if not reconcile(args.glob, redis_conn_args, args.mdts):
        sys.exit(1)

if __name__ == "__main__":
    main()
