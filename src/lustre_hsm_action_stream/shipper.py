# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-action-shipper

A production-hardened, lightweight daemon for shipping Lustre HSM events
from MDT 'actions' logs to Redis streams.
"""

import argparse
import os
import sys
import time
import glob
import yaml
import json
import logging
import hashlib
import signal
import redis

# Import the parser from the local package.
from .parser import parse_action_line

SHUTDOWN_REQUESTED = False

def handle_shutdown_signal(signum, frame):
    """Handles SIGTERM/SIGINT for graceful exit after the current event cycle."""
    global SHUTDOWN_REQUESTED
    if not SHUTDOWN_REQUESTED:
        logging.info(f"Shutdown signal ({signal.Signals(signum).name}) received. Finishing current poll cycle.")
        SHUTDOWN_REQUESTED = True
    else:
        logging.warning("Multiple shutdown signals. Forcing immediate exit.")
        sys.exit(1)

# The absolute default path for the installed system.
DEFAULT_CONFIG_PATH = "/etc/lustre-hsm-action-stream/hsm_action_shipper.yaml"

def load_config(path):
    """Loads and validates the YAML config file."""
    try:
        with open(path) as f:
            conf = yaml.safe_load(f)
        required = [
            'mdt_watch_glob', 'cache_path', 'poll_interval',
            'redis_host', 'redis_port', 'redis_db', 'redis_stream_name'
        ]
        for key in required:
            if key not in conf:
                raise KeyError(f"Missing required config key: '{key}' in {path}")
        return conf
    except Exception as e:
        print(f"FATAL: Could not load or validate config {path}: {e}", file=sys.stderr)
        sys.exit(1)

class RedisConnector:
    """Redis connection wrapper for automatic reconnect."""
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self.client = None

    def connect(self):
        """Attempts connection, with exponential backoff & diagnostic logging."""
        if self.client:
            try:
                self.client.ping()
                return
            except redis.exceptions.ConnectionError:
                logging.warning("Redis connection lost. Reconnecting...")
                self.client = None
        delay = 1
        while self.client is None and not SHUTDOWN_REQUESTED:
            try:
                logging.info(f"Connecting to Redis at {self.host}:{self.port} db={self.db} ...")
                client = redis.Redis(
                    host=self.host, port=self.port, db=self.db,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    decode_responses=False
                )
                client.ping()
                self.client = client
                logging.info("Connected to Redis!")
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                logging.error(f"Redis connect failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, 30)

    def get_client(self):
        """Ensures active Redis client is returned, reconnected if needed."""
        self.connect()
        return self.client

def load_cache(path):
    """Loads event cache from disk. Returns an empty dict on first run."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            loaded_data = json.load(f)
        # Reconstruct the tuple key safely without using eval()
        cache = {}
        for k, v in loaded_data.items():
            parts = k.split(':', 2)
            # key_tuple: (mdt_name, cat_idx, rec_idx)
            key_tuple = (parts[0], int(parts[1]), int(parts[2]))
            cache[key_tuple] = v
        return cache
    except Exception as e:
        logging.warning(f"Could not load cache file {path}, starting fresh. Error: {e}")
        return {}

def save_cache(cache, path):
    """Atomically saves cache to disk using a safe, temporary file."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        # Serialize tuple key to a safe string format 'mdt:cat_idx:rec_idx'
        serializable_cache = {f"{k[0]}:{k[1]}:{k[2]}": v for k, v in cache.items()}
        with open(tmp_path, 'w') as f:
            json.dump(serializable_cache, f)
        os.replace(tmp_path, path)
    except Exception as e:
        logging.error(f"Failed to save cache file {path}: {e}")

def process_action_files(mdt_files, cache):
    """
    Scans HSM actions files for live actions, detects new/updated events.
    Returns a list of events to ship and a set of all keys seen this cycle.
    """
    events_to_ship = []
    keys_seen_this_cycle = set()
    for action_file in mdt_files:
        # Assumes path is like .../<mdt_name>/hsm/actions
        mdt_name = os.path.basename(os.path.dirname(os.path.dirname(action_file)))
        try:
            with open(action_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    data = parse_action_line(line)
                    if not data:
                        continue

                    cat_idx, rec_idx = data['cat_idx'], data['rec_idx']
                    key = (mdt_name, cat_idx, rec_idx)
                    keys_seen_this_cycle.add(key)
                    line_hash = hashlib.md5(line.encode()).hexdigest()

                    cached_entry = cache.get(key)
                    current_entry = {'hash': line_hash, 'action': data.get('action'),
                                     'fid': data.get('fid'), 'status': data.get('status')}

                    event_type = None
                    if not cached_entry:
                        event_type = "NEW"
                    elif cached_entry['hash'] != line_hash:
                        event_type = "UPDATE"

                    if event_type:
                        cache[key] = current_entry
                        event = {
                            "event_type": event_type,
                            "action": data.get('action'),
                            "fid": data.get('fid'),
                            "status": data.get('status'),
                            "mdt": mdt_name,
                            "cat_idx": cat_idx,
                            "rec_idx": rec_idx,
                            "timestamp": int(time.time()),
                            "raw": line
                        }
                        events_to_ship.append(event)
        except Exception as e:
            logging.error(f"Error processing {action_file}: {e}")
    return events_to_ship, keys_seen_this_cycle

def purge_and_emit(cache, keys_seen):
    """
    Finds and emits PURGED events for actions that no longer exist.
    Removes purged keys from the cache.
    """
    purged_keys = set(cache.keys()) - keys_seen
    events = []
    for key in purged_keys:
        cached_info = cache.pop(key)  # Atomically get and remove
        mdt_name, cat_idx, rec_idx = key
        event = {
            "event_type": "PURGED",
            "mdt": mdt_name,
            "cat_idx": cat_idx,
            "rec_idx": rec_idx,
            "timestamp": int(time.time()),
            "action": cached_info.get('action', 'UNKNOWN'),
            "fid": cached_info.get('fid'),
            "status": "PURGED"
        }
        events.append(event)
    return events

def watcher_and_shipper_loop(conf, run_once=False):
    """Main daemon event scanner/shipper loop."""
    cache = load_cache(conf['cache_path'])
    logging.info(f"Loaded {len(cache)} actions from persistent cache.")
    redis_connector = RedisConnector(conf['redis_host'], conf['redis_port'], conf['redis_db'])
    cycle_num = 0
    logging.info(f"Started watcher on MDT glob: {conf['mdt_watch_glob']} every {conf['poll_interval']}s.")

    while not SHUTDOWN_REQUESTED:
        start_time = time.time()
        cycle_num += 1
        logging.info(f"[Cycle {cycle_num}] Starting scan...")

        mdt_files = glob.glob(conf['mdt_watch_glob'])
        if not mdt_files:
            logging.warning(f"No files found for glob: {conf['mdt_watch_glob']}")

        new_and_updated_events, keys_seen = process_action_files(mdt_files, cache)
        purged_events = purge_and_emit(cache, keys_seen)
        events_to_ship = new_and_updated_events + purged_events

        shipped_successfully = True
        if events_to_ship:
            r = redis_connector.get_client()
            if not r:
                logging.error("Failed to get Redis client. Events will be re-processed next cycle.")
                shipped_successfully = False
            else:
                try:
                    pipe = r.pipeline()
                    for event in events_to_ship:
                        pipe.xadd(conf['redis_stream_name'], {"data": json.dumps(event)})
                    pipe.execute()
                    logging.info(f"Shipped {len(events_to_ship)} events to Redis stream '{conf['redis_stream_name']}'.")
                except Exception as e:
                    logging.error(f"Failed to ship events to Redis: {e}. They will be re-processed next cycle.")
                    shipped_successfully = False
        else:
            logging.info(f"[Cycle {cycle_num}] No changes detected.")

        # Save the cache *only* after successful processing and shipping.
        # If shipping fails, we don't save, so the next cycle retries the same events.
        if shipped_successfully:
            save_cache(cache, conf['cache_path'])

        # If in run-once mode for testing, exit the loop after one cycle.
        if run_once:
            logging.info("Run-once mode complete. Exiting.")
            break

        # Sleep until the next poll interval, allowing for early exit
        elapsed = time.time() - start_time
        logging.info(f"Scan complete in {elapsed:.2f}s, cache size: {len(cache)}. Next scan in {max(0, conf['poll_interval'] - elapsed):.1f}s.")
        sleep_time = max(0, conf['poll_interval'] - elapsed)

        # Granular sleep for responsive shutdown
        t_slept = 0
        while t_slept < sleep_time:
            if SHUTDOWN_REQUESTED:
                break
            time.sleep(1)
            t_slept += 1

    save_cache(cache, conf['cache_path'])
    logging.info("Graceful shutdown complete. Final cache state saved.")

def main():
    """Main entry point for the hsm-action-shipper executable."""
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    parser = argparse.ArgumentParser(
        description="Lustre HSM Actions Shipper: Watches Lustre MDT actions logs and ships events to a Redis stream."
    )
    parser.add_argument(
        "-c", "--config", default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config file (default: {DEFAULT_CONFIG_PATH})"
    )
    parser.add_argument(
        "--run-once", action="store_true",
        help="Perform a single poll cycle and then exit. Intended for testing."
    )
    args = parser.parse_args()

    conf = load_config(args.config)

    log_format = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
    log_level = conf.get("log_level", "INFO").upper()

    log_file = conf.get("log_file")
    if log_file:
        logging.basicConfig(level=log_level, format=log_format, filename=log_file)
    else:
        logging.basicConfig(level=log_level, format=log_format, stream=sys.stdout)

    try:
        watcher_and_shipper_loop(conf, run_once=args.run_once)
    except Exception as e:
        logging.critical(f"Unhandled exception in main execution: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
