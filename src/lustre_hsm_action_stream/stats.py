# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-stats

A stateless metrics collector for the Lustre HSM action stream. It replays the
stream to generate an accurate snapshot of the current state, and outputs the
result as a JSON object, ideal for Telegraf's json_v2 parser.
"""

import sys
import yaml
import json
import logging
import redis
import time
import argparse
from collections import Counter

class StatsGenerator:
    """Calculates and prints metrics by replaying the HSM action stream."""
    def __init__(self, conf):
        self.conf = conf
        self.live_actions = {}

    def _id_to_timestamp(self, redis_id):
        """Converts a Redis stream ID string to a unix timestamp (float seconds)."""
        try:
            return int(redis_id.split('-')[0]) / 1000.0
        except (ValueError, IndexError):
            return 0.0

    def _process_one_event(self, msg_id, event_data):
        """Updates the in-memory 'live_actions' dictionary based on a single event."""
        try:
            event = json.loads(event_data[b'data'])
            key = (event['mdt'], event['cat_idx'], event['rec_idx'])
            
            if event['event_type'] in ("NEW", "UPDATE"):
                # Store the necessary attributes for later aggregation.
                self.live_actions[key] = {
                    'id': msg_id.decode(),
                    'mdt': event.get('mdt'),
                    'action': event.get('action'),
                    'status': event.get('status')
                }
            elif event['event_type'] == "PURGED":
                self.live_actions.pop(key, None)
        except (json.JSONDecodeError, KeyError) as e:
            logging.warning(f"Could not parse event {msg_id.decode()}, skipping. Error: {e}")

    def run(self):
        """Main execution flow: connect, replay stream, calculate, print."""
        logging.info("Performing a full stream replay for maximum accuracy.")

        try:
            r = redis.Redis(
                host=self.conf['redis_host'], port=self.conf['redis_port'], db=self.conf['redis_db'],
                socket_connect_timeout=5, decode_responses=False
            )
            r.ping()
        except Exception as e:
            logging.error(f"Could not connect to Redis: {e}")
            return

        stream_name = self.conf['redis_stream_name']
        
        # --- NEW: Get stream boundary metrics directly from Redis ---
        stream_total_age_seconds = 0
        newest_entry_age_seconds = 0
        try:
            stream_info = r.xinfo_stream(stream_name)
            first_id_bytes = stream_info.get('first-entry', [None, None])[0]
            last_id_bytes = stream_info.get('last-generated-id')

            if first_id_bytes and last_id_bytes:
                first_ts = self._id_to_timestamp(first_id_bytes.decode())
                last_ts = self._id_to_timestamp(last_id_bytes.decode())
                stream_total_age_seconds = int(last_ts - first_ts)
                newest_entry_age_seconds = int(time.time() - last_ts)
        except redis.exceptions.ResponseError: # Stream empty/doesn't exist
            pass
        except Exception as e:
            logging.warning(f"Could not retrieve stream boundary metrics: {e}")

        # --- Replay stream to build live state ---
        events_processed = 0
        last_id = '0-0'
        while True:
            # Read in large chunks for efficiency.
            response = r.xread({stream_name: last_id}, count=10000)
            if not response:
                break
            for _, messages in response:
                for msg_id, event_data in messages:
                    events_processed += 1
                    self._process_one_event(msg_id, event_data)
                    last_id = msg_id.decode()
        
        logging.info(f"Processed {events_processed} events, found {len(self.live_actions)} live actions.")

        # --- Aggregate live action breakdowns ---
        breakdown_counter = Counter(
            (v.get('mdt'), v.get('action'), v.get('status'))
            for v in self.live_actions.values()
        )
        # Convert the counter into a flat list of dictionaries, which is an
        # ideal format for Telegraf's json_v2 parser to iterate over.
        breakdown_list = []
        for (mdt, action, status), count in breakdown_counter.items():
            breakdown_list.append({
                "mdt": mdt or "unknown",
                "action": action or "unknown",
                "status": status or "unknown",
                "count": count
            })

        # --- Calculate oldest live action age ---
        oldest_live_action_age_seconds = 0
        if self.live_actions:
            oldest_id = min(v['id'] for v in self.live_actions.values())
            oldest_ts = self._id_to_timestamp(oldest_id)
            if oldest_ts > 0:
                oldest_live_action_age_seconds = int(time.time() - oldest_ts)
        
        # --- Prepare Final JSON Output ---
        summary_metrics = {
            # Gauges for the current state
            "total_live_actions": len(self.live_actions),
            "oldest_live_action_age_seconds": oldest_live_action_age_seconds,
            
            # Gauges for the stream's health
            "stream_total_age_seconds": stream_total_age_seconds,
            "newest_entry_age_seconds": newest_entry_age_seconds,

            # Internal metric for this run
            "events_processed_in_run": events_processed
        }
        
        final_output = {
            "summary": summary_metrics,
            "breakdown": breakdown_list
        }
        
        json.dump(final_output, sys.stdout)

def main():
    parser = argparse.ArgumentParser(
        description="A stateless metrics collector for the HSM action stream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-c", "--config", default="/etc/lustre-hsm-action-stream/hsm_stream_stats.yaml", help="Path to YAML config file.")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    args = parser.parse_args()

    try:
        with open(args.config) as f:
            conf = yaml.safe_load(f)
    except Exception as e:
        print(f"FATAL: Could not load config {args.config}: {e}", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s:%(name)s:%(message)s", stream=sys.stderr)

    stats_generator = StatsGenerator(conf)
    stats_generator.run()

if __name__ == "__main__":
    main()
