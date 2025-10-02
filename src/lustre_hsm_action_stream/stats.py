# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-stats

A stateless metrics collector for the Lustre HSM action stream. It replays
all discovered hsm:actions:* streams to generate a snapshot of the current
state, and outputs the result as a JSON object to stdout.
"""

import sys
import yaml
import json
import logging
import time
import argparse
from collections import Counter
import redis

# Import the new high-level consumer API
from .consumer import StreamReader

class StatsGenerator:
    """Calculates and prints metrics by replaying all HSM action streams."""
    def __init__(self, conf):
        self.conf = conf
        self.live_actions = {}
        self.events_processed = 0

    def _id_to_timestamp(self, redis_id_str):
        """Converts a Redis stream ID string to a unix timestamp (float seconds)."""
        try:
            return int(redis_id_str.split('-')[0]) / 1000.0
        except (ValueError, IndexError):
            return 0.0

    def _process_one_event(self, event_id, event_data):
        """Updates the in-memory 'live_actions' dictionary based on a single event."""
        key = (event_data['mdt'], event_data['cat_idx'], event_data['rec_idx'])

        if event_data['event_type'] in ("NEW", "UPDATE"):
            self.live_actions[key] = {
                'id': event_id,
                'mdt': event_data.get('mdt'),
                'action': event_data.get('action'),
                'status': event_data.get('status')
            }
        elif event_data['event_type'] == "PURGED":
            self.live_actions.pop(key, None)

    def _get_stream_health_metrics(self, r, stream_names):
        """Gets length and age metrics for a list of streams."""
        health_metrics = {'total_stream_entries': 0, 'streams': {}}
        now = time.time()
        for name in stream_names:
            try:
                length = r.xlen(name)
                health_metrics['total_stream_entries'] += length
                info = r.xinfo_stream(name)
                first = info.get('first-entry')
                last_id = info.get('last-generated-id')
                age, newest_age = 0, 0
                if first and last_id:
                    age = int(self._id_to_timestamp(last_id.decode()) - self._id_to_timestamp(first[0].decode()))
                    newest_age = int(now - self._id_to_timestamp(last_id.decode()))

                health_metrics['streams'][name] = {
                    'length': length,
                    'age_seconds': age,
                    'newest_entry_age_seconds': newest_age
                }
            except redis.exceptions.ResponseError:
                logging.warning(f"Stream '{name}' disappeared during health check.")
            except Exception as e:
                logging.warning(f"Could not get health for stream '{name}': {e}")
        return health_metrics

    def run(self):
        """Main execution flow: connect, replay streams, calculate, print JSON."""
        logging.info("Starting a full stream replay for metrics generation.")

        # The StreamReader handles connection and discovery internally.
        reader = StreamReader(
            host=self.conf['redis_host'],
            port=self.conf['redis_port'],
            db=self.conf['redis_db'],
            prefix=self.conf['redis_stream_prefix']
        )

        # --- Replay ALL streams to build global live state ---
        logging.info(f"Discovering and replaying streams with prefix '{self.conf['redis_stream_prefix']}:*'.")
        
        # Use a small block_ms. The generator yields None when history is done.
        for event in reader.events(from_beginning=True, block_ms=500):
            if not event:
                # No more historical events, replay is complete.
                break
            try:
                self._process_one_event(event.id, event.data)
                self.events_processed += 1
            except (KeyError, TypeError) as e:
                logging.warning(f"Could not parse event {event.id}, skipping. Error: {e}")

        logging.info(f"Replay complete. Processed {self.events_processed:,} events, found {len(self.live_actions):,} live actions.")

        # --- Get stream health metrics (requires a separate, direct Redis connection) ---
        health_metrics = {}
        streams_list = []
        if reader.streams:
            try:
                direct_r = redis.Redis(host=self.conf['redis_host'], port=self.conf['redis_port'], db=self.conf['redis_db'])
                health_metrics = self._get_stream_health_metrics(direct_r, reader.streams)
                # Convert the dictionary of streams into a list of objects
                for stream_name, metrics in health_metrics.get('streams', {}).items():
                    mdt_name = stream_name.split(':')[-1]
                    streams_list.append({"mdt": mdt_name, **metrics})
            except Exception as e:
                logging.error(f"Could not connect to Redis for health metrics: {e}")

        # --- Calculate summary and breakdown metrics ---
        breakdown_counter = Counter((v.get('mdt'), v.get('action'), v.get('status')) for v in self.live_actions.values())
        breakdown_list = [{"mdt": m or 'N/A', "action": a or 'N/A', "status": s or 'N/A', "count": c} 
                          for (m, a, s), c in sorted(breakdown_counter.items())]

        oldest_live_action_age_seconds = 0
        if self.live_actions:
            oldest_id = min(v['id'] for v in self.live_actions.values())
            oldest_ts = self._id_to_timestamp(oldest_id)
            if oldest_ts > 0:
                oldest_live_action_age_seconds = int(time.time() - oldest_ts)

        summary_metrics = {
            "total_live_actions": len(self.live_actions),
            "oldest_live_action_age_seconds": oldest_live_action_age_seconds,
            "total_events_replayed": self.events_processed,
            "total_stream_entries": health_metrics.get('total_stream_entries', 0)
        }
        # --- Assemble the final top-level JSON object ---
        final_output = {
            "summary": summary_metrics,
            "streams": streams_list,
            "breakdown": breakdown_list
        }
        json.dump(final_output, sys.stdout, indent=2)


def main():
    parser = argparse.ArgumentParser(description="A stateless metrics collector for the HSM action stream.")
    parser.add_argument("-c", "--config", default="/etc/lustre-hsm-action-stream/hsm_stream_stats.yaml", help="Path to config file.")
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level for stderr output.")
    parser.add_argument("--log-file", help="Redirect logging output to a file instead of stderr.")
    args = parser.parse_args()

    # --- Configure logging to go to stderr by default ---
    log_kwargs = {
        "level": args.log_level.upper(),
        "format": "%(asctime)s %(levelname)s:%(name)s:%(message)s",
    }
    if args.log_file:
        log_kwargs["filename"] = args.log_file
    else:
        log_kwargs["stream"] = sys.stderr

    logging.basicConfig(**log_kwargs)

    try:
        with open(args.config) as f:
            conf = yaml.safe_load(f)
        if 'redis_stream_prefix' not in conf:
            raise KeyError("Config must contain 'redis_stream_prefix'")
    except Exception as e:
        logging.critical(f"Could not load or validate config {args.config}: {e}")
        sys.exit(1)

    stats_generator = StatsGenerator(conf)
    stats_generator.run()

if __name__ == "__main__":
    main()
