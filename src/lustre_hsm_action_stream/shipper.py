# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-action-shipper

A self-healing, lightweight daemon for shipping Lustre HSM events from
MDT 'actions' logs to dedicated per-MDT Redis streams. Includes integrated
stream validation, backfilling, and garbage collection.
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
from typing import Tuple

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
    """Loads and validates the YAML config file, providing defaults for new keys for backward compatibility."""
    try:
        with open(path) as f:
            conf = yaml.safe_load(f)

        # Handle renamed/new keys for backward compatibility
        if 'aggressive_trim_threshold' in conf and 'trim_chunk_size' not in conf:
            logging.warning("Config key 'aggressive_trim_threshold' is deprecated. Please use 'trim_chunk_size'.")
            conf['trim_chunk_size'] = conf.pop('aggressive_trim_threshold')

        if 'trim_chunk_size' not in conf:
            conf['trim_chunk_size'] = 1000
            logging.info("Config key 'trim_chunk_size' not found. Defaulting to 1000.")

        if 'use_approximate_trimming' not in conf:
            conf['use_approximate_trimming'] = True
            logging.info("Config key 'use_approximate_trimming' not found. Defaulting to 'true'.")

        required = [
            'mdt_watch_glob', 'cache_path', 'poll_interval', 'reconcile_interval',
            'redis_host', 'redis_port', 'redis_db', 'redis_stream_prefix',
            'trim_chunk_size', 'use_approximate_trimming'
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
        dirpath = os.path.dirname(path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        tmp_path = f"{path}.tmp"
        serializable_cache = {f"{k[0]}:{k[1]}:{k[2]}": v for k, v in cache.items()}
        with open(tmp_path, 'w') as f:
            json.dump(serializable_cache, f)
        os.replace(tmp_path, path)
    except Exception as e:
        logging.error(f"Failed to save cache file {path}: {e}")

### This helper prevents errors from files changing during a read.
def _read_file_safely(path: str) -> Tuple[bytes, bool]:
    """Reads a file while checking for modification during the read."""
    try:
        st1 = os.stat(path)
        with open(path, 'rb') as f:
            data = f.read()
        st2 = os.stat(path)
        if st1.st_mtime_ns != st2.st_mtime_ns or st1.st_size != st2.st_size:
            logging.warning(f"File '{path}' changed during read. Purge calculations will be skipped for its MDT this cycle.")
            return data, False
        return data, True
    except FileNotFoundError:
        # This is an expected condition (e.g., MDT failover), not a warning.
        return b'', True
    except Exception as e:
        logging.error(f"Error reading file '{path}': {e}")
        return b'', False

# --- Core Shipper Logic ---
def do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector):
    """
    Performs one cycle of polling, change detection, and event shipping using a robust transactional model.
    """
    logging.info("[Shipper] Starting poll cycle...")
    start_time = time.time()
    stream_prefix = conf['redis_stream_prefix']

    mdt_files = glob.glob(conf['mdt_watch_glob'])
    if not mdt_files:
        logging.warning(f"No files found for glob: {conf['mdt_watch_glob']}")

    events_to_ship = []
    pending_cache_updates = {}
    keys_seen_this_cycle = set()
    locally_discovered_mdts = set()
    unstable_mdts = set()

    with cache_lock:
        for action_file in mdt_files:
            mdt_name = os.path.basename(os.path.dirname(os.path.dirname(action_file)))
            locally_discovered_mdts.add(mdt_name)

            file_content, is_stable = _read_file_safely(action_file)
            if not is_stable:
                unstable_mdts.add(mdt_name)

            try:
                for line_bytes in file_content.splitlines():
                    line = line_bytes.decode('utf-8', 'replace').strip()
                    if not line:
                        continue

                    # Ensure parsed data is valid and has a 'fid'.
                    data = parse_action_line(line)
                    if not data or 'fid' not in data:
                        continue

                    action_key = f"{data['fid']}:{data['action']}"
                    key = (mdt_name, data['cat_idx'], data['rec_idx'])
                    keys_seen_this_cycle.add(key)
                    line_hash = hashlib.md5(line.encode()).hexdigest()

                    if cache.get(key, {}).get('hash') != line_hash:
                        event_type = "UPDATE" if key in cache else "NEW"
                        new_cache_entry = {'hash': line_hash, 'action': data.get('action'), 'fid': data.get('fid'), 'action_key': action_key}
                        pending_cache_updates[key] = new_cache_entry
                        event = {**data, "event_type": event_type, "mdt": mdt_name, "timestamp": int(start_time), "raw": line, 'action_key': action_key}
                        events_to_ship.append(event)
            except Exception as e:
                logging.error(f"[Shipper] Error processing content of {action_file}: {e}", exc_info=True)

        purged_keys = set(cache.keys()) - keys_seen_this_cycle
        for key in list(purged_keys):
            if key[0] in unstable_mdts:
                logging.warning(f"[Shipper] Deferring purge of {key} because MDT {key[0]} was unstable this cycle.")
                continue

            cached_info = cache.get(key)
            pending_cache_updates[key] = None

            # Ensure the PURGED event gets a valid action_key
            action_key = cached_info.get('action_key') if cached_info else None
            if not action_key and cached_info and cached_info.get('fid') and cached_info.get('action'):
                 action_key = f"{cached_info.get('fid')}:{cached_info.get('action')}"

            # For orphans, we create a placeholder action_key
            if not action_key:
                action_key = f"unknown:{key[1]}:{key[2]}"

            event_payload = {
                "event_type": "PURGED", "mdt": key[0], "cat_idx": key[1], "rec_idx": key[2],
                "timestamp": int(start_time), "status": "PURGED", "action_key": action_key
            }
            if cached_info:
                 event_payload.update(cached_info)

            events_to_ship.append(event_payload)

    if events_to_ship:
        r = redis_connector.get_client()
        if r and not SHUTDOWN_EVENT.is_set():
            try:
                pipe = r.pipeline()
                for event in events_to_ship:
                    stream_name = f"{stream_prefix}:{event['mdt']}"
                    pipe.xadd(stream_name, {"data": json.dumps(event)})
                pipe.execute()
                logging.info(f"[Shipper] Shipped {len(events_to_ship)} events.")
                with cache_lock:
                    for key, value in pending_cache_updates.items():
                        if value is None:
                            cache.pop(key, None)
                        else:
                            cache[key] = value
                    save_cache(cache, conf['cache_path'])
            except Exception as e:
                logging.error(f"[Shipper] Failed to ship events: {e}. Cache not updated. Will retry next cycle.")
    else:
        logging.debug("[Shipper] No changes detected.")

    with cache_lock:
        return copy.deepcopy(cache), locally_discovered_mdts

# --- Core Maintenance Logic ---
def _parse_stream_id(s: str) -> Tuple[int, int]:
    try:
        ms, seq = s.split('-', 1)
        return int(ms), int(seq)
    except (ValueError, IndexError):
        return 0, 0

def _replay_stream_and_get_state(stream_name, r):
    """
    Replays an entire stream to rebuild the current state of live actions.
    Returns a dictionary of {action_key: stream_id} and the last seen stream ID.
    """
    live_actions = {}
    last_id = '0-0'
    final_id = '0-0'
    total_processed = 0

    while not SHUTDOWN_EVENT.is_set():
        try:
            response = r.xread({stream_name: last_id}, count=1000)
            if not response:
                break

            messages = response[0][1]
            if not messages:
                break

            for msg_id, msg_data in messages:
                # Always advance last_id to avoid looping on bad entries
                msg_id_str = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
                last_id = msg_id_str
                total_processed += 1

                try:
                    raw = msg_data.get(b'data')
                    if raw is None:
                        raw = msg_data.get('data')
                    if raw is None:
                        logging.warning(f"Skipping message {msg_id_str} in '{stream_name}': missing 'data' field.")
                        continue

                    payload = raw.decode('utf-8', 'replace') if isinstance(raw, (bytes, bytearray)) else raw
                    event = json.loads(payload)

                    action_key = event.get('action_key')
                    status = event.get('status')

                    if not action_key:
                        logging.warning(f"Skipping message {msg_id_str} in '{stream_name}': missing 'action_key'.")
                        continue

                    if status == "PURGED":
                        live_actions.pop(action_key, None)
                    else:
                        live_actions[action_key] = msg_id_str

                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                    logging.warning(f"Skipping malformed message {msg_id_str} in '{stream_name}': {e}")

            final_id = last_id

        except redis.exceptions.ResponseError as e:
            logging.warning(f"Could not read from stream '{stream_name}', it may not exist. Error: {e}")
            return {}, '0-0' # Return empty state

    logging.debug(f"Replayed {total_processed} events from '{stream_name}'. Found {len(live_actions)} live actions.")
    return live_actions, final_id

def _validate_stream_consistency(conf, ground_truth_snapshot, mdt, live_action_keys, r):
    stream_name = f"{conf['redis_stream_prefix']}:{mdt}"
    logging.info(f"[Maintenance] Validating stream consistency for: {stream_name}")

    # Build a set of action_keys from the ground truth cache to match the format from the stream replay.
    truth_action_keys = set()
    for key_tuple, cache_entry in ground_truth_snapshot.items():
        if key_tuple[0] == mdt:
            # An action_key might not be in old cache entries, so we construct it.
            action_key = cache_entry.get('action_key')
            if not action_key and cache_entry.get('fid') and cache_entry.get('action'):
                action_key = f"{cache_entry.get('fid')}:{cache_entry.get('action')}"

            if action_key:
                truth_action_keys.add(action_key)

    # Now we compare a set of strings to a set of strings.
    orphans_in_stream = live_action_keys - truth_action_keys
    missing_from_stream = truth_action_keys - live_action_keys # Not used yet, but correct for future use

    if orphans_in_stream:
        logging.warning(f"[Maintenance] Found {len(orphans_in_stream)} orphan(s) for {mdt}. Injecting corrective PURGED events.")
        maintenance_events = []
        for action_key in orphans_in_stream:
            # We can't know the original cat/rec_idx, but we have the essential action_key.
            event = {
                "event_type": "PURGED",
                "mdt": mdt,
                "action_key": action_key, # The most important field
                "status": "PURGED",
                "timestamp": int(time.time()),
                "source": "maintenance"
            }
            maintenance_events.append(event)

        if maintenance_events:
            try:
                pipe = r.pipeline()
                for event in maintenance_events:
                    pipe.xadd(stream_name, {"data": json.dumps(event)})
                pipe.execute()
                logging.info(f"[Maintenance] Shipped {len(maintenance_events)} maintenance events for {mdt}.")
            except Exception as e:
                logging.error(f"[Maintenance] Failed to ship maintenance events for {mdt}: {e}")
    else:
        logging.info(f"[Maintenance] Stream for {mdt} is consistent with ground truth.")

    # Return the set of orphan action_keys so the caller can update its in-memory state.
    return orphans_in_stream

def _trim_stream(conf, mdt, live_action_ids, r):
    """Trims old events from a stream based on the oldest remaining live action ID."""
    stream_name = f"{conf['redis_stream_prefix']}:{mdt}"
    logging.info(f"[Maintenance] Starting partial garbage collection for: {stream_name}")

    # This function is only called when live_action_ids is NOT empty.

    valid_ids = [stream_id for stream_id in live_action_ids.values() if _parse_stream_id(stream_id) != (0, 0)]
    if not valid_ids:
        logging.warning(f"[Maintenance] No valid stream IDs found among live actions for '{stream_name}'. Skipping trim.")
        return

    oldest_live_id = min(valid_ids, key=_parse_stream_id)
    approximate = conf.get('use_approximate_trimming', True)
    logging.info(f"[Maintenance] Oldest live action ID is {oldest_live_id}. "
                 f"Trimming entries older than this (Approximate Mode: {approximate}).")

    try:
        chunk_size = conf.get('trim_chunk_size', 1000)
        total_deleted = 0
        loop_count = 0
        WARN_AFTER_LOOPS = 100

        while not SHUTDOWN_EVENT.is_set():
            loop_count += 1
            deleted_count = r.xtrim(stream_name, minid=oldest_live_id, limit=chunk_size, approximate=approximate)

            # Defensive check for unexpected return values from the client library.
            if not isinstance(deleted_count, int) or deleted_count < 0:
                logging.error(f"[Maintenance] CRITICAL: XTRIM returned an invalid value: {deleted_count}. Stopping trim for {stream_name}.")
                break

            # The only safe exit condition is when Redis reports it deleted 0 entries.
            if deleted_count == 0:
                break

            total_deleted += deleted_count
            logging.debug(f"[Maintenance] Trimmed a chunk of {deleted_count:,} from '{stream_name}'. Re-trimming...")

            if loop_count == WARN_AFTER_LOOPS:
                logging.warning(f"[Maintenance] Partial trim for '{stream_name}' has run {loop_count} times, indicating a very large backlog is being processed.")

        if total_deleted > 0:
            logging.info(f"[Maintenance] Garbage collection for '{stream_name}' complete. Total entries removed: {total_deleted:,}.")
        else:
            logging.info(f"[Maintenance] No old entries needed trimming before ID {oldest_live_id}.")
    except redis.exceptions.ResponseError as e:
        logging.error(f"[Maintenance] Redis error during chunked trim for {mdt}. Check if '{oldest_live_id}' is a valid ID. Error: {e}")
    except Exception as e:
        logging.error(f"[Maintenance] Unhandled error during chunked stream trimming for {mdt}: {e}")

def run_maintenance_cycle(conf, ground_truth_snapshot, locally_managed_mdts, redis_connector):
    """Orchestrates the full, efficient maintenance process: replay once, then act."""
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
        stream_name = f"{conf['redis_stream_prefix']}:{mdt}"
        try:
            live_action_ids, _ = _replay_stream_and_get_state(stream_name, r)

            orphaned_keys = _validate_stream_consistency(conf, ground_truth_snapshot, mdt, live_action_ids.keys(), r)

            # Update our in-memory view of live actions *after* validation.
            for key in orphaned_keys:
                live_action_ids.pop(key, None)

            # If, after reconciliation, no actions are considered live, it's safe to trim everything.
            if not live_action_ids:
                logging.info(f"[Maintenance] No live actions remain for '{stream_name}' after reconciliation. Clearing all stream history...")
                try:
                    chunk_size = conf.get('trim_chunk_size', 1000)
                    total_deleted = 0
                    loop_count = 0
                    WARN_AFTER_LOOPS = 100

                    while not SHUTDOWN_EVENT.is_set():
                        loop_count += 1
                        deleted_count = r.xtrim(stream_name, maxlen=0, limit=chunk_size)

                        # Defensive check for unexpected return values.
                        if not isinstance(deleted_count, int) or deleted_count < 0:
                            logging.error(f"[Maintenance] CRITICAL: XTRIM (maxlen=0) returned an invalid value: {deleted_count}. Stopping trim for {stream_name}.")
                            break

                        # The only safe exit condition is when Redis returns 0.
                        if deleted_count == 0:
                            break

                        total_deleted += deleted_count
                        logging.debug(f"[Maintenance] Trimmed a chunk of {deleted_count:,} from '{stream_name}'. Re-trimming...")

                        if loop_count == WARN_AFTER_LOOPS:
                            logging.warning(f"[Maintenance] Full trim for '{stream_name}' has run {loop_count} times, indicating a very large backlog is being cleared.")

                    if total_deleted > 0:
                        logging.info(f"[Maintenance] Full garbage collection for '{stream_name}' complete. Total entries removed: {total_deleted:,}.")
                    else:
                        logging.info(f"[Maintenance] Stream '{stream_name}' was already empty or cleared in a prior pass. No aggressive GC needed.")
                except Exception as e:
                    logging.error(f"[Maintenance] Error during full stream trim for {mdt}: {e}")
            else:
                # If live actions *do* remain, call the standard partial trimmer.
                _trim_stream(conf, mdt, live_action_ids, r)

        except redis.exceptions.ResponseError as e:
            logging.warning(f"[Maintenance] Redis error during maintenance for '{mdt}', skipping. Error: {e}")
        except Exception as e:
            logging.error(f"[Maintenance] Unhandled error during maintenance for {mdt}: {e}", exc_info=True)

    logging.info(f"[Maintenance] Full maintenance cycle finished in {time.time() - start_time:.2f}s.")

# --- Thread Worker Functions and Main Entry Point ---
def shipper_thread_worker(conf, maintenance_queue, cache, cache_lock):
    logging.info("Shipper thread started.")
    redis_connector = RedisConnector(conf['redis_host'], conf['redis_port'], conf['redis_db'])
    reconcile_interval, poll_interval = conf['reconcile_interval'], conf['poll_interval']
    # Stagger the first maintenance run
    last_maintenance_time = time.time() - reconcile_interval + 60
    while not SHUTDOWN_EVENT.is_set():
        start_time = time.time()
        snapshot, mdt_names = do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector)
        if time.time() - last_maintenance_time > reconcile_interval:
            logging.info("[Shipper] Triggering background maintenance task.")
            try:
                # Pass the snapshot of the cache and the MDTs this host is responsible for
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
            try:
                run_maintenance_cycle(conf, ground_truth_snapshot, mdt_names, redis_connector)
            finally:
                maintenance_queue.task_done()
        except queue.Empty:
            continue
        except Exception:
            logging.error("[Maintenance] Unhandled exception in maintenance worker loop.", exc_info=True)
    logging.info("Maintenance thread gracefully shut down.")

def main():
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    parser = argparse.ArgumentParser(description="Lustre HSM Actions Shipper with built-in maintenance.")
    parser.add_argument("-c", "--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--run-once", action="store_true", help="Perform one poll/ship cycle and exit.")
    parser.add_argument("--maintenance-now", action="store_true", help="In run-once mode, also perform a maintenance cycle.")
    args = parser.parse_args()
    conf = load_config(args.config)
    logging.basicConfig(level=conf.get("log_level", "INFO").upper(), format="%(asctime)s %(levelname)s:%(name)s:%(threadName)s:%(message)s")

    cache_lock = threading.Lock()
    cache = load_cache(conf['cache_path'])
    redis_connector = RedisConnector(conf['redis_host'], conf['redis_port'], conf['redis_db'])

    if args.run_once or args.maintenance_now:
        logging.info("Executing in run-once mode.")
        snapshot, mdt_names = do_shipper_poll_cycle(conf, cache, cache_lock, redis_connector)
        if args.maintenance_now:
            run_maintenance_cycle(conf, snapshot, mdt_names, redis_connector)
        logging.info("Run-once execution complete.")
        sys.exit(0)

    maintenance_queue = queue.Queue(maxsize=1)
    shipper = threading.Thread(target=shipper_thread_worker, name="Shipper", args=(conf, maintenance_queue, cache, cache_lock))
    maintenance_thread = threading.Thread(target=maintenance_thread_worker, name="Maintenance", args=(conf, maintenance_queue, redis_connector))
    try:
        shipper.start()
        maintenance_thread.start()
        while shipper.is_alive() and maintenance_thread.is_alive():
            shipper.join(timeout=1.0)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt in main thread, initiating shutdown.")
        if not SHUTDOWN_EVENT.is_set():
            SHUTDOWN_EVENT.set()
    except Exception as e:
        logging.critical(f"Unhandled exception in main thread: {e}", exc_info=True)
        if not SHUTDOWN_EVENT.is_set():
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
