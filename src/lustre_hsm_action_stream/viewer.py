# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-action-top

A 'top'-like dashboard for viewing live Lustre HSM action statistics by consuming
the Redis event stream. Includes a 'diff' column to highlight real-time changes.
"""

import os
import sys
import redis
import json
import time
import threading
from collections import Counter
import argparse

class HSMStateTracker:
    """
    Connects to Redis and maintains an in-memory state of all HSM actions
    by consuming the event stream in a background thread.
    """
    def __init__(self, redis_conn_args, stream_name):
        self.redis_conn_args = redis_conn_args
        self.stream_name = stream_name
        self.live_actions = {}
        self.stats = Counter()
        self.lock = threading.Lock()
        self.is_connected = False
        self.last_event_ts = 0
        self.status_message = "Initializing..."
        self.previous_summary = Counter()

    def event_consumer_thread(self):
        """Background thread to continuously consume events from Redis."""
        while True:
            try:
                r_args = self.redis_conn_args.copy()
                r_args['socket_connect_timeout'] = 5
                r = redis.Redis(**r_args, decode_responses=False)
                r.ping()
                with self.lock:
                    self.is_connected = True
                self.process_stream(r)
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                with self.lock:
                    self.is_connected = False
                    self.status_message = f"Redis connection error: {e}. Retrying..."
                time.sleep(5)
            except Exception as e:
                with self.lock:
                    self.status_message = f"Error in consumer thread: {e}"
                time.sleep(5)

    def process_stream(self, r):
        """Replay stream from beginning for history, then block for live updates."""
        with self.lock:
            self.status_message = "Replaying history to build initial state..."
        last_id = '0-0'
        events_in_bootstrap = 0
        while True:
            response = r.xread({self.stream_name: last_id}, count=50000)
            if not response:
                break
            with self.lock:
                for _, messages in response:
                    for msg_id, event_data in messages:
                        events_in_bootstrap += 1
                        self._process_one_event(event_data)
                        last_id = msg_id
        with self.lock:
            self.status_message = f"Bootstrap complete ({events_in_bootstrap:,} events). Listening for live updates..."

        while True:
            response = r.xread({self.stream_name: last_id}, count=1000, block=5000)
            if not response:
                continue
            with self.lock:
                for _, messages in response:
                    for msg_id, event_data in messages:
                        self._process_one_event(event_data)
                        last_id = msg_id

    def _process_one_event(self, event_data):
        """Update live state with one event."""
        try:
            event = json.loads(event_data[b'data'])
            key = (event['mdt'], event['cat_idx'], event['rec_idx'])
            self.stats['events_processed'] += 1
            self.last_event_ts = time.time()
            if event['event_type'] in ("NEW", "UPDATE"):
                self.live_actions[key] = event
            elif event['event_type'] == "PURGED":
                if key in self.live_actions:
                    del self.live_actions[key]
        except (json.JSONDecodeError, KeyError):
            self.stats['parse_errors'] += 1

    def get_summary(self):
        """Calculates a summary including diffs from previous screen update."""
        with self.lock:
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
                "is_connected": self.is_connected,
                "total_events": self.stats['events_processed'],
                "last_event_ago": int(time.time() - self.last_event_ts) if self.last_event_ts else -1,
                "status_message": self.status_message,
                "parse_errors": self.stats['parse_errors']
            }

def main():
    """Main entry point for the hsm-action-top executable."""
    parser = argparse.ArgumentParser(description="A 'top'-like dashboard for HSM events.")
    parser.add_argument('--host', default="localhost", help="Redis server host.")
    parser.add_argument('--port', type=int, default=6379, help="Redis server port.")
    parser.add_argument('--db', type=int, default=1, help="Redis database number.")
    parser.add_argument('--stream', default="hsm:actions", help="Redis stream name.")
    parser.add_argument('--interval', type=float, default=2.0, help="Refresh interval in seconds.")
    parser.add_argument(
        "--run-once", action="store_true",
        help="Draw a single frame and then exit. Intended for testing."
    )
    args = parser.parse_args()

    redis_conn_args = {'host': args.host, 'port': args.port, 'db': args.db}

    tracker = HSMStateTracker(redis_conn_args, args.stream)
    consumer = threading.Thread(target=tracker.event_consumer_thread, daemon=True)
    consumer.start()

    # Wait a moment for the consumer thread to potentially connect and bootstrap
    # especially for a run-once test.
    time.sleep(0.5)

    try:
        while True:
            # Only clear the screen if we are NOT in run-once mode.
            if not args.run_once:
                os.system('clear' if os.name == 'posix' else 'cls')
            else:
                # In test mode, just print a separator for clarity.
                print("\n--- Viewer Run-Once Frame ---")

            summary = tracker.get_summary()

            conn_status = "Connected" if summary['is_connected'] else "DISCONNECTED"
            print("--- Lustre HSM Action Dashboard ---")
            print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')} | Redis: {conn_status}")
            print(f"Viewer Status: {summary['status_message']}")

            last_event_str = f"{summary['last_event_ago']}s ago" if summary['last_event_ago'] != -1 else "Never"
            print(f"Live Actions: {summary['live_action_count']:,} | Last Event: {last_event_str}")

            print("\n--- Live Action Count by (MDT, Action, Status) ---")
            summary_table = summary['summary_table']

            if not summary_table:
                print("No live actions detected.")
            else:
                max_mdt = max((len(item['key'][0]) for item in summary_table), default=5)
                max_act = max((len(item['key'][1]) for item in summary_table), default=6)
                header = f"{'MDT':<{max_mdt}} | {'ACTION':<{max_act}} | {'STATUS':<10} | {'COUNT':>10} {'DIFF':<10}"
                print(header)
                print("-" * len(header))

                for item in summary_table:
                    mdt, action, status = item['key']
                    count = item['count']
                    diff = item['diff']
                    status_str = status if status is not None else "N/A"
                    diff_str = ""
                    if diff > 0:
                        diff_str = f"(+{diff:,})"
                    elif diff < 0:
                        diff_str = f"({diff:,})"
                    count_and_diff = f"{count:>{10},}"
                    print(f"{mdt:<{max_mdt}} | {action:<{max_act}} | {status_str:<10} | {count_and_diff} {diff_str}")

            if summary['parse_errors'] > 0:
                print(f"\nWARNING: {summary['parse_errors']:,} stream events failed to parse.")

            # If in run-once mode, break the loop after the first frame.
            if args.run_once:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)

if __name__ == "__main__":
    main()
