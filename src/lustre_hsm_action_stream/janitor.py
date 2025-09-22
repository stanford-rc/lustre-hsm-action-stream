# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-janitor

A state-driven garbage collector for the Lustre HSM Redis action stream.
It safely trims old events that are no longer part of any active operation.
Includes self-healing logic to reap "zombie" actions where a PURGED event
was missed.
"""

import os
import sys
import yaml
import json
import logging
import redis
import time
import threading
import argparse

def load_config(path):
    """Loads and validates the YAML configuration file for the janitor."""
    try:
        with open(path) as f:
            conf = yaml.safe_load(f)
        # Check for keys required by the janitor specifically.
        required_keys = [
            'redis_host', 'redis_port', 'redis_db', 'redis_stream_name',
            'operation_interval_seconds'
        ]
        for key in required_keys:
            if key not in conf:
                raise KeyError(f"Required configuration key '{key}' not found in {path}")
        return conf
    except Exception as e:
        print(f"FATAL: Could not load or validate config {path}: {e}", file=sys.stderr)
        sys.exit(1)


class Janitor:
    """
    Manages the state and lifecycle of the stream janitor daemon.

    This class encapsulates all state (like the set of live actions) and
    logic, avoiding global variables and improving thread safety.
    """
    def __init__(self, conf, dry_run=False):
        self.conf = conf
        self.dry_run = dry_run
        
        # This dictionary is the core of the janitor's state. It tracks all
        # actions believed to be "live" in the Lustre HSM system.
        # Key: (mdt, cat_idx, rec_idx) tuple
        # Value: {'id': str, 'status': str, 'last_seen': float} dictionary
        self.live_actions = {}
        
        # A lock to protect access to the 'live_actions' dictionary from
        # the consumer thread and the main janitor task.
        self.state_lock = threading.Lock()
        
        # An event flag to signal when the initial historical replay of the
        # Redis stream is complete. The janitor will not perform any trimming
        # until this is set.
        self.is_bootstrapped = threading.Event()
        
        self.redis_client = None

    def _connect_redis(self):
        """Establishes and pings a Redis connection for the consumer thread."""
        self.redis_client = redis.Redis(
            host=self.conf['redis_host'], port=self.conf['redis_port'], db=self.conf['redis_db'],
            socket_connect_timeout=5, decode_responses=False
        )
        self.redis_client.ping()

    def _get_stream_tail_id(self):
        """
        Gets the latest stream ID before starting the bootstrap. This defines
        a clear "end point" for the historical replay phase.
        """
        try:
            # XINFO STREAM provides metadata about the stream, including the last ID.
            info = self.redis_client.xinfo_stream(self.conf['redis_stream_name'])
            return info.get('last-generated-id', b'0-0').decode()
        except redis.exceptions.ResponseError: # Stream does not exist yet.
            logging.info("Stream does not exist yet. Starting from the beginning.")
            return '0-0'
        except Exception as e:
            logging.warning(f"Could not get stream tail ID, will start from beginning. Error: {e}")
            return '0-0'

    def _process_one_event(self, msg_id, event_data):
        """Updates the internal 'live_actions' state based on a single event."""
        try:
            event = json.loads(event_data[b'data'])
            key = (event['mdt'], event['cat_idx'], event['rec_idx'])
            msg_id_str = msg_id.decode()

            if event['event_type'] in ("NEW", "UPDATE"):
                existing_action = self.live_actions.get(key)
                new_status = event.get('status')

                # Only update the 'last_seen' timestamp if the action is brand new
                # or if its status is actually changing.
                if not existing_action or existing_action.get('status') != new_status:
                    self.live_actions[key] = {
                        'id': msg_id_str,
                        'status': new_status,
                        'last_seen': time.time()
                    }
                else:
                    # If we are just re-processing an old event during bootstrap,
                    # only update the event ID. DO NOT change 'last_seen'.
                    # This preserves the original time the action entered its terminal state.
                    self.live_actions[key]['id'] = msg_id_str

            elif event['event_type'] == "PURGED":
                self.live_actions.pop(key, None)
            
            return msg_id_str
        except (json.JSONDecodeError, KeyError) as e:
            logging.warning(f"Could not parse event, skipping. Error: {e}. Raw: {event_data}")
            return None

    def event_consumer_loop(self):
        """
        A background thread that consumes events from Redis to maintain the
        'live_actions' state dictionary. It runs continuously.
        """
        logging.info("Redis consumer thread started.")
        while True:
            try:
                logging.info("Connecting to Redis...")
                self._connect_redis()
                logging.info("Redis connection successful.")

                stream_name = self.conf['redis_stream_name']
                tail_id = self._get_stream_tail_id()
                logging.info(f"Bootstrap mode: replaying stream up to approx. tail ID: {tail_id}")
                last_id = '0-0'
                events_in_bootstrap = 0

                # --- Bootstrap Phase: Replay history as fast as possible ---
                while True:
                    # Read without blocking to consume quickly.
                    response = self.redis_client.xread({stream_name: last_id}, count=10000)
                    if not response:
                        break # Reached the end of the stream.
                    
                    with self.state_lock:
                        for _, messages in response:
                            for msg_id, event_data in messages:
                                events_in_bootstrap += 1
                                last_id = self._process_one_event(msg_id, event_data) or last_id
                    
                    # Consider bootstrap done if we've processed up to or past the original tail.
                    if last_id >= tail_id:
                        break
                
                # Signal to the main janitor task that it's safe to start working.
                self.is_bootstrapped.set()
                logging.info(f"Bootstrap complete. Processed {events_in_bootstrap:,} events.")

                # --- Live Tailing Phase: Block and wait for new events ---
                while True:
                    response = self.redis_client.xread({stream_name: last_id}, count=1000, block=5000)
                    if not response:
                        continue # Timed out, loop again.
                    
                    with self.state_lock:
                        for _, messages in response:
                            for msg_id, event_data in messages:
                                last_id = self._process_one_event(msg_id, event_data) or last_id
            
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                logging.warning("Redis connection lost. Retrying in 5s...")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Critical error in consumer thread: {e}", exc_info=True)
                time.sleep(5)

    def janitor_task(self, run_once=False):
        """
        The main periodic task. It waits for bootstrap, then enters a loop to
        analyze the live action set and trim the Redis stream accordingly.
        """
        logging.info("Waiting for initial bootstrap from Redis stream...")
        self.is_bootstrapped.wait()
        logging.info("Initial state synchronized. Starting cleanup tasks.")

        while True:
            try:
                # --- SELF-HEALING LOGIC ---
                # This block handles "zombie" actions where the shipper might have missed
                # a PURGED event (e.g., due to downtime). If an action has been in a
                # terminal state (SUCCEED/FAILED) for too long, we assume it's a zombie
                # and reap it from our internal state. This prevents garbage collection
                # from halting indefinitely.
                timeout = self.conf.get('terminal_state_timeout_seconds', 300)
                now = time.time()
                reaped_keys = []
                with self.state_lock:
                    for key, action_data in self.live_actions.items():
                        if action_data.get('status') in ('SUCCEED', 'FAILED'):
                            if (now - action_data.get('last_seen', now)) > timeout:
                                logging.warning(
                                    f"Reaping stale terminal action {key} (status: {action_data['status']}) "
                                    f"due to timeout ({timeout}s). The PURGED event was likely missed."
                                )
                                reaped_keys.append(key)
                    # Safely remove the reaped keys outside the iteration
                    for key in reaped_keys:
                        self.live_actions.pop(key)

                # --- TRIMMING LOGIC ---
                with self.state_lock:
                    # Get the Redis stream IDs of all actions still considered live.
                    current_live_ids = [v['id'] for v in self.live_actions.values()]

                if not current_live_ids:
                    logging.info("No live actions found. Nothing to trim against.")
                    oldest_referenced_id = None
                else:
                    # The oldest event we MUST keep is the one with the minimum ID.
                    oldest_referenced_id = min(current_live_ids)
                
                live_count = len(current_live_ids)
                oldest_id_display = oldest_referenced_id or "(none)"
                logging.info(f"Analysis complete. Live actions: {live_count:,}. Oldest active event ID: {oldest_id_display}")

                if oldest_referenced_id:
                    if self.dry_run:
                        logging.warning(f"DRY-RUN: Would have trimmed stream up to ID: {oldest_referenced_id}")
                    else:
                        try:
                            logging.info(f"Trimming stream up to ID: {oldest_referenced_id}")
                            # Use a new connection for the blocking trim command to avoid interfering with the consumer.
                            r_trim = redis.Redis(host=self.conf['redis_host'], port=self.conf['redis_port'], db=self.conf['redis_db'])
                            # XTRIM with MINID is the command to garbage collect the stream.
                            deleted_count = r_trim.xtrim(self.conf['redis_stream_name'], minid=oldest_referenced_id)
                            logging.info(f"Successfully trimmed {deleted_count:,} old entries from the stream.")
                        except Exception as e:
                            logging.error(f"Failed to trim stream: {e}")
            except Exception as e:
                logging.error(f"Error in janitor task: {e}", exc_info=True)
            
            # If in run-once mode for testing, exit the loop after one cycle.
            if run_once:
                logging.info("Run-once mode complete. Exiting janitor task.")
                break

            time.sleep(self.conf['operation_interval_seconds'])

def main():
    """Main entry point for the hsm-stream-janitor executable."""
    parser = argparse.ArgumentParser(
        description="A state-driven garbage collector for the Lustre HSM Redis stream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-c", "--config", default="/etc/lustre-hsm-action-stream/hsm_stream_janitor.yaml", help="Path to the YAML configuration file.")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Override the log level from the config file.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate trimming without deleting any data from Redis.")
    parser.add_argument("--run-once", action="store_true", help="Perform one cleanup cycle and exit. For testing.")
    args = parser.parse_args()
    
    conf = load_config(args.config)
    
    log_level = args.log_level or conf.get("log_level", "INFO").upper()
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s:%(name)s:%(threadName)s:%(message)s")
    
    logging.info(f"Config loaded from {args.config}")
    if args.dry_run:
        logging.warning("DRY-RUN MODE ENABLED. No data will be deleted from Redis.")
    
    janitor = Janitor(conf, args.dry_run)
    # The consumer runs in a background daemon thread.
    consumer_thread = threading.Thread(target=janitor.event_consumer_loop, daemon=True, name="RedisConsumer")
    
    try:
        consumer_thread.start()
        # The main janitor task runs in the foreground.
        janitor.janitor_task(run_once=args.run_once)
    except KeyboardInterrupt:
        logging.info("Shutdown requested. Exiting.")
    except Exception as e:
        logging.critical(f"Unhandled exception in main execution: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
