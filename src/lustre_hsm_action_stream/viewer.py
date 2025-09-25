# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-action-top

A 'top'-like dashboard for viewing live Lustre HSM action statistics by consuming
all discovered Redis event streams. Includes a 'diff' column to highlight changes.
"""

import os
import sys
import json
import time
import threading
from collections import Counter
import argparse
import logging

# Import the new high-level consumer API
from .consumer import StreamReader

class HSMStateTracker:
    """
    Maintains an in-memory state of all HSM actions by consuming events
    from a StreamReader in a background thread.
    """
    def __init__(self, reader):
        """
        Initializes the tracker with a configured StreamReader instance.
        """
        self.reader = reader
        self.live_actions = {}
        self.stats = Counter()
        self.lock = threading.Lock()
        self.last_event_ts = 0
        self.status_message = "Initializing..."
        self.previous_summary = Counter()

    def event_consumer_thread(self):
        """Background thread to continuously consume events via the StreamReader."""

        # The StreamReader handles all connection and reconnection logic.
        # We just need to iterate over its event generator.

        self.status_message = "Replaying history to build initial state..."

        # Use a small block_ms to distinguish historical replay from live tailing.
        for event in self.reader.events(from_beginning=True, block_ms=500):
            with self.lock:
                if event:
                    self._process_one_event(event)
                else:
                    # The generator yielded None, meaning historical replay is complete.
                    self.status_message = f"Bootstrap complete ({self.stats['events_processed']:,} events). Listening for live updates..."
                    break # Move to the live tailing loop

        # Now, block indefinitely for live events.
        for event in self.reader.events(block_ms=0):
            with self.lock:
                if event:
                    self._process_one_event(event)

    def _process_one_event(self, event):
        """
        Update live state with one event from the StreamReader.
        The `event` is a `StreamEvent` namedtuple.
        """
        try:
            e_data = event.data
            key = (e_data['mdt'], e_data['cat_idx'], e_data['rec_idx'])
            self.stats['events_processed'] += 1
            self.last_event_ts = time.time()
            if e_data['event_type'] in ("NEW", "UPDATE"):
                self.live_actions[key] = e_data
            elif e_data['event_type'] == "PURGED":
                if key in self.live_actions:
                    del self.live_actions[key]
        except (KeyError, TypeError):
            self.stats['parse_errors'] += 1

    def get_summary(self):
        """Calculates a summary of the current state, including diffs."""
        with self.lock:
            # Use the robust, thread-safe flag from the reader
            is_connected = self.reader.is_connected

            current_summary = Counter(
                (d.get('mdt', '?'), d.get('action', '?'), d.get('status'))
                for d in self.live_actions.values()
            )
            summary_table = []
            all_keys = set(current_summary.keys()) | set(self.previous_summary.keys())

            for key in sorted(list(all_keys)):
                current_count = current_summary.get(key, 0)
                previous_count = self.previous_summary.get(key, 0)
                diff = current_count - previous_count
                if current_count > 0 or diff != 0:
                    summary_table.append({
                        "key": key, "count": current_count, "diff": diff
                    })
            self.previous_summary = current_summary

            return {
                "live_action_count": len(self.live_actions),
                "summary_table": summary_table,
                "is_connected": is_connected,
                "total_events": self.stats['events_processed'],
                "last_event_ago": int(time.time() - self.last_event_ts) if self.last_event_ts else -1,
                "status_message": self.status_message,
                "parse_errors": self.stats['parse_errors']
            }

def draw_dashboard(tracker, run_once):
    """Draws a single frame of the dashboard to the console."""
    if not run_once:
        os.system('clear' if os.name == 'posix' else 'cls')
    else:
        print("\n--- Viewer Run-Once Frame ---")

    summary = tracker.get_summary()

    conn_status = "Connected" if summary['is_connected'] else "DISCONNECTED"
    print("--- Lustre HSM Action Dashboard ---")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')} | Redis: {conn_status}")
    print(f"Viewer Status: {summary['status_message']}")

    last_event_str = f"{summary['last_event_ago']}s ago" if summary['last_event_ago'] != -1 else "Never"
    print(f"Live Actions: {summary['live_action_count']:,} | Total Events Processed: {summary['total_events']:,} | Last Event: {last_event_str}")

    print("\n--- Live Action Count by (MDT, Action, Status) ---")
    summary_table = summary['summary_table']

    if not summary_table:
        print("No live actions detected.")
    else:
        max_mdt = max((len(str(item['key'][0])) for item in summary_table), default=5)
        max_act = max((len(str(item['key'][1])) for item in summary_table), default=6)
        header = f"{'MDT':<{max_mdt}} | {'ACTION':<{max_act}} | {'STATUS':<10} | {'COUNT':>10} {'DIFF':<10}"
        print(header)
        print("-" * len(header))

        for item in summary_table:
            mdt, action, status = item['key']
            count, diff = item['count'], item['diff']
            status_str = status if status is not None else "N/A"
            diff_str = f"(+{diff:,})" if diff > 0 else f"({diff:,})" if diff < 0 else ""
            print(f"{mdt:<{max_mdt}} | {action:<{max_act}} | {status_str:<10} | {count:>{10},d} {diff_str}")

    if summary['parse_errors'] > 0:
        print(f"\nWARNING: {summary['parse_errors']:,} stream events failed to parse.")

def main():
    """Main entry point for the hsm-action-top executable."""
    parser = argparse.ArgumentParser(description="A 'top'-like dashboard for HSM events.")
    parser.add_argument('--host', default="localhost", help="Redis server host.")
    parser.add_argument('--port', type=int, default=6379, help="Redis server port.")
    parser.add_argument('--db', type=int, default=1, help="Redis database number.")
    parser.add_argument('--stream-prefix', default="hsm:actions", help="Prefix of Redis Streams to discover.")
    parser.add_argument('--interval', type=float, default=2.0, help="Refresh interval in seconds.")
    parser.add_argument("--run-once", action="store_true", help="Draw a single frame and then exit.")
    args = parser.parse_args()

    logging.basicConfig(filename='/tmp/hsm-action-top.log', level=logging.INFO,
                        format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

    reader = StreamReader(
        host=args.host,
        port=args.port,
        db=args.db,
        prefix=args.stream_prefix
    )
    tracker = HSMStateTracker(reader)

    # --- Special synchronous logic for --run-once ---
    if args.run_once:
        logging.info("Executing viewer in run-once mode.")
        tracker.status_message = "Replaying history for run-once snapshot..."

        # Sequentially replay all historical events until the stream is exhausted.
        # A short block_ms allows the generator to yield None when history is done.
        for event in reader.events(from_beginning=True, block_ms=200):
            if event:
                with tracker.lock:
                    tracker._process_one_event(event)
            else:
                # No more events in the historical replay, we're done.
                break

        with tracker.lock:
             tracker.status_message = f"Snapshot complete ({tracker.stats['events_processed']:,} events)."

        # Now draw the single, complete frame and exit.
        draw_dashboard(tracker, True)
        sys.exit(0)

    # Default daemon mode (threaded)
    consumer = threading.Thread(target=tracker.event_consumer_thread, daemon=True)
    consumer.start()

    time.sleep(0.5) # A small sleep is fine for daemon startup cosmetic effect.

    try:
        while True:
            draw_dashboard(tracker, False)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)

if __name__ == "__main__":
    main()
