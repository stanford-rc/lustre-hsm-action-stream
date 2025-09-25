# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-action-shipper

A self-healing, lightweight daemon for shipping Lustre HSM events from
MDT 'actions' logs to dedicated per-MDT Redis streams. Includes integrated
stream validation and garbage collection.
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
import threading
import queue
import copy

from .parser import parse_action_line

# Global shutdown event for coordinating graceful termination of threads.
SHUTDOWN_EVENT = threading.Event()

def handle_shutdown_signal(signum, frame):
    """Handles SIGTERM/SIGINT, setting the global shutdown event."""
    if not SHUTDOWN_EVENT.is_set():
        logging.info(f"Shutdown signal ({signal.Signals(signum).name}) received. Stopping all threads...")
        SHUTDOWN_EVENT.set()
    else:
        logging.warning("Multiple shutdown signals received. Forcing exit.")
        sys.exit(1)

DEFAULT_CONFIG_PATH = "/etc/lustre-hsm-action-stream/hsm_action_shipper.yaml"

def load_config(path):
    """Loads and validates the YAML config file."""
    try:
        with open(path) as f:
            conf = yaml.safe_load(f)
        required = [
            'mdt_watch_glob', 'cache_path', 'poll_interval', 'reconcile_interval',
            'aggressive_trim_threshold', 'redis_host', 'redis_port', 'redis_db',
            'redis_stream_prefix'
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
        if self.client:
            try:
                self.client.ping()
                return
            except redis.exceptions.ConnectionError:
                logging.warning("Redis connection lost. Reconnecting...")
                self.client = None
        delay = 1
        while self.client is None and not SHUTDOWN_EVENT.is_set():
            try:
                logging.info(f"Connecting to Redis at {self.host}:{self.port} db={self.db} ...")
                client = redis.Redis(
                    host=self.host, port=self.port, db=self.db,
                    socket_connect_timeout=5, socket_timeout=5,
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
        self.connect()
        return self.client

def load_cache(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            loaded_data = json.load(f)
        cache = {}
        for k, v in loaded_data.items():
            parts = k.split(':', 2)
            key_tuple = (parts[0], int(parts[1]), int(parts[2]))
            cache[key_tuple] = v
        return cache
    except Exception as e:
        logging.warning(f"Could not load cache file {path}, starting fresh. Error: {e}")
        return {}

def save_cache(cache, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        serializable_cache = {f"{k[0]}:{k[1]}:{k[2]}": v for k, v in cache.items()}
        with open(tmp_path, 'w') as f:
            json.dump(serializable_cache, f)
        os.replace(tmp_path, path)
    except Exception as e:
        logging.error(f"Failed to save cache file {path}: {e}")

# --- Core Logic Functions ---

def do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector):
    """
    Performs one cycle of polling, change detection, and event shipping.
    Returns a snapshot of the cache and a set of locally discovered MDTs.
    """
    logging.info("[Shipper] Starting poll cycle...")
    start_time = time.time()
    stream_prefix = conf['redis_stream_prefix']

    mdt_files = glob.glob(conf['mdt_watch_glob'])
    if not mdt_files:
        logging.warning(f"No files found for glob: {conf['mdt_watch_glob']}")

    events_to_ship = []
    keys_seen_this_cycle = set()
    locally_discovered_mdts = set()

    with cache_lock:
        for action_file in mdt_files:
            mdt_name = os.path.basename(os.path.dirname(os.path.dirname(action_file)))
            locally_discovered_mdts.add(mdt_name)
            try:
                with open(action_file, 'r') as f:
                    for line in f:
                        data = parse_action_line(line.strip())
                        if not data:
                            continue
                        key = (mdt_name, data['cat_idx'], data['rec_idx'])
                        keys_seen_this_cycle.add(key)
                        line_hash = hashlib.md5(line.strip().encode()).hexdigest()
                        if cache.get(key, {}).get('hash') != line_hash:
                            event_type = "UPDATE" if key in cache else "NEW"
                            cache[key] = {'hash': line_hash, 'action': data.get('action'), 'fid': data.get('fid')}
                            event = {**data, "event_type": event_type, "mdt": mdt_name, "timestamp": int(start_time), "raw": line.strip()}
                            events_to_ship.append(event)
            except Exception as e:
                logging.error(f"[Shipper] Error processing {action_file}: {e}")
        purged_keys = set(cache.keys()) - keys_seen_this_cycle
        for key in list(purged_keys):
            cached_info = cache.pop(key)
            events_to_ship.append({
                "event_type": "PURGED", "mdt": key[0], "cat_idx": key[1], "rec_idx": key[2],
                "timestamp": int(start_time), "status": "PURGED", **cached_info
            })
    if events_to_ship:
        r = redis_connector.get_client()
        if r and not SHUTDOWN_EVENT.is_set():
            try:
                pipe = r.pipeline()
                for event in events_to_ship:
                    pipe.xadd(f"{stream_prefix}:{event['mdt']}", {"data": json.dumps(event)})
                pipe.execute()
                logging.info(f"[Shipper] Shipped {len(events_to_ship)} events.")
                with cache_lock:
                    save_cache(cache, conf['cache_path'])
            except Exception as e:
                logging.error(f"[Shipper] Failed to ship events: {e}. Will retry next cycle.")

    else:
        logging.info("[Shipper] No changes detected.")
    with cache_lock:
        return copy.deepcopy(cache), locally_discovered_mdts

def _validate_stream_consistency(conf, ground_truth_snapshot, mdt, r):
    """(Maintenance Step 1) Ensures stream consistency for a single MDT."""
    stream_name = f"{conf['redis_stream_prefix']}:{mdt}"
    logging.info(f"[Maintenance] Validating stream consistency for: {stream_name}")
    try:
        stream_state_keys = set()
        last_id = '0-0'
        while not SHUTDOWN_EVENT.is_set():
            response = r.xread({stream_name: last_id}, count=10000)
            if not response:
                break
            for _, messages in response:
                for msg_id_bytes, event_data in messages:
                    event = json.loads(event_data[b'data']); key = (event['mdt'], event['cat_idx'], event['rec_idx'])
                    if event['event_type'] in ("NEW", "UPDATE"):
                        stream_state_keys.add(key)
                    elif event['event_type'] == "PURGED":
                        stream_state_keys.discard(key)
                    last_id = msg_id_bytes
        truth_keys_for_mdt = {k for k in ground_truth_snapshot if k[0] == mdt}
        orphans_in_stream = stream_state_keys - truth_keys_for_mdt
        if orphans_in_stream:
            logging.warning(f"[Maintenance] Found {len(orphans_in_stream)} orphan(s) for {mdt}. Injecting corrective PURGED events.")
            pipe = r.pipeline()
            for key in orphans_in_stream:
                event = {
                        "event_type": "PURGED", "mdt": key[0], "cat_idx": key[1], "rec_idx": key[2],
                        "timestamp": int(time.time()), "status": "PURGED", "action": "RECONCILED"
                }
                pipe.xadd(stream_name, {"data": json.dumps(event)})
            pipe.execute()
        else:
            logging.info(f"[Maintenance] Stream for {mdt} is consistent with ground truth.")
    except redis.exceptions.ResponseError:
        logging.warning(f"[Maintenance] Stream {stream_name} not found during validation. Skipping.")
    except Exception as e:
        logging.error(f"[Maintenance] Error during stream validation for {mdt}: {e}")

def _trim_stream(conf, mdt, r):
    """(Maintenance Step 2) Trims old events from a single MDT's stream."""
    stream_name = f"{conf['redis_stream_prefix']}:{mdt}"
    logging.info(f"[Maintenance] Starting garbage collection for: {stream_name}")
    try:
        live_action_ids = {}
        last_id = '0-0'
        while not SHUTDOWN_EVENT.is_set():
            response = r.xread({stream_name: last_id}, count=10000)
            if not response:
                break
            for _, messages in response:
                for msg_id_bytes, event_data in messages:
                    msg_id = msg_id_bytes.decode()
                    event = json.loads(event_data[b'data'])
                    key = (event['mdt'], event['cat_idx'], event['rec_idx'])
                    if event['event_type'] in ("NEW", "UPDATE"):
                        live_action_ids[key] = msg_id
                    elif event['event_type'] == "PURGED":
                        live_action_ids.pop(key, None)
                    last_id = msg_id_bytes

        if not live_action_ids:
            if last_id != b'0-0':
                deleted_count = r.xtrim(stream_name, minid=last_id.decode())
                if deleted_count > 0:
                    logging.info(f"[Maintenance] No live actions found. Trimmed {deleted_count:,} entries from stream '{stream_name}'.")
            return

        oldest_live_id = min(live_action_ids.values())
        threshold = conf['aggressive_trim_threshold']
        total_deleted = 0
        while not SHUTDOWN_EVENT.is_set():
            deleted_count = r.xtrim(stream_name, minid=oldest_live_id)
            total_deleted += deleted_count
            if deleted_count > 0:
                logging.info(f"[Maintenance] Trimmed {deleted_count:,} old entries from stream '{stream_name}'.")
            if deleted_count < threshold:
                break
            logging.info(f"[Maintenance] Aggressive trim: More than {threshold} entries deleted. Re-trimming immediately.")
        if total_deleted > 0:
            logging.info(f"[Maintenance] Garbage collection for '{stream_name}' complete. Total entries removed: {total_deleted:,}.")

    except redis.exceptions.ResponseError:
        logging.debug(f"[Maintenance] Stream {stream_name} not found during trimming. Skipping.")
    except Exception as e:
        logging.error(f"[Maintenance] Error during stream trimming for {mdt}: {e}")

def run_maintenance_cycle(conf, ground_truth_snapshot, locally_managed_mdts, redis_connector):
    """Orchestrates the full maintenance process: validation and trimming."""
    logging.info("[Maintenance] Starting full maintenance cycle.")
    start_time = time.time()
    r = redis_connector.get_client()
    if not r or SHUTDOWN_EVENT.is_set():
        logging.error("[Maintenance] No Redis connection or shutdown requested. Aborting cycle.")
        return
    logging.info(f"[Maintenance] Will perform maintenance on locally managed MDTs: {locally_managed_mdts or 'None'}")
    for mdt in locally_managed_mdts:
        if SHUTDOWN_EVENT.is_set():
            break
        _validate_stream_consistency(conf, ground_truth_snapshot, mdt, r)
        _trim_stream(conf, mdt, r)
    logging.info(f"[Maintenance] Full maintenance cycle finished in {time.time() - start_time:.2f}s.")

# --- Thread Worker Functions (Daemon Mode) ---
def shipper_thread_worker(conf, maintenance_queue, cache, cache_lock):
    logging.info("Shipper thread started."); redis_connector = RedisConnector(conf['redis_host'], conf['redis_port'], conf['redis_db'])
    reconcile_interval = conf['reconcile_interval']; poll_interval = conf['poll_interval']; last_maintenance_time = time.time() - reconcile_interval + 60
    while not SHUTDOWN_EVENT.is_set():
        start_time = time.time()
        snapshot, mdt_names = do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector)
        if time.time() - last_maintenance_time > reconcile_interval:
            logging.info("[Shipper] Triggering background maintenance task.")
            try:
                maintenance_queue.put_nowait((snapshot, mdt_names))
                last_maintenance_time = time.time()
            except queue.Full:
                logging.warning("[Shipper] Maintenance queue is full. Skipping this trigger.")
        elapsed = time.time() - start_time
        logging.info(f"Poll cycle complete in {elapsed:.2f}s. Next in {max(0, poll_interval - elapsed):.1f}s.")
        SHUTDOWN_EVENT.wait(max(0, poll_interval - elapsed))
    logging.info("Shipper thread gracefully shut down.")

def maintenance_thread_worker(conf, maintenance_queue, redis_connector):
    logging.info("Maintenance thread started and waiting for tasks.")
    while not SHUTDOWN_EVENT.is_set():
        try:
            ground_truth_snapshot, mdt_names = maintenance_queue.get(timeout=5)
            run_maintenance_cycle(conf, ground_truth_snapshot, mdt_names, redis_connector)
            maintenance_queue.task_done()
        except queue.Empty:
            continue
    logging.info("Maintenance thread gracefully shut down.")

# --- Main Entry Point ---
def main():
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    parser = argparse.ArgumentParser(description="Lustre HSM Actions Shipper with built-in maintenance.")
    parser.add_argument("-c", "--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--run-once", action="store_true", help="Perform one poll/ship cycle and exit.")
    parser.add_argument("--maintenance-now", action="store_true", help="In run-once mode, also perform a maintenance cycle.")
    args = parser.parse_args()
    conf = load_config(args.config); logging.basicConfig(level=conf.get("log_level", "INFO").upper(), format="%(asctime)s %(levelname)s:%(name)s:%(threadName)s:%(message)s")
    cache_lock = threading.Lock(); cache = load_cache(conf['cache_path'])
    redis_connector = RedisConnector(conf['redis_host'], conf['redis_port'], conf['redis_db'])

    if args.run_once or args.maintenance_now:
        logging.info("Executing in run-once mode.")
        snapshot, mdt_names = do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector)
        if args.maintenance_now:
            run_maintenance_cycle(conf, snapshot, mdt_names, redis_connector)
        logging.info("Run-once execution complete."); sys.exit(0)

    maintenance_queue = queue.Queue(maxsize=1)
    shipper = threading.Thread(target=shipper_thread_worker, name="Shipper", args=(conf, maintenance_queue, cache, cache_lock))
    maintenance_thread = threading.Thread(target=maintenance_thread_worker, name="Maintenance", args=(conf, maintenance_queue, redis_connector))
    try:
        shipper.start()
        maintenance_thread.start()
        while shipper.is_alive():
            shipper.join(timeout=1.0)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt in main thread, initiating shutdown.")
        SHUTDOWN_EVENT.set()
    except Exception as e:
        logging.critical(f"Unhandled exception in main thread: {e}", exc_info=True)
        SHUTDOWN_EVENT.set()
        sys.exit(1)
    finally:
        logging.info("Waiting for threads to join...")
        shipper.join()
        maintenance_thread.join()
        with cache_lock:
            save_cache(cache, conf['cache_path'])
        logging.info("All threads have finished. Final cache state saved.")

if __name__ == "__main__":
    main()
