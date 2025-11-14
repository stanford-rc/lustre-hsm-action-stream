#!/usr/bin/env python3
"""
lrestore_ahead_client.py

Copyright (C) 2025
     The Board of Trustees of the Leland Stanford Junior University
Written by Stephane Thiell <sthiell@stanford.edu>

Licensed under GPL v3 (see https://www.gnu.org/licenses/).

Listens to Lustre HSM RESTORE events from the hsm-action-stream and triggers
intelligent, paced restore-ahead operations for large, multi-part objects.
This daemon is designed to be robust for production use, featuring:
- Event-driven pacing to prevent overwhelming tape drives.
- A metrics API for Telegraf monitoring (with non-resetting counters).
- Efficient, thread-safe database handling with performance indexing.
- Graceful shutdown on SIGINT/SIGTERM.
- Resource limits to prevent memory exhaustion.
- Self-validating configuration checks.

--- Metrics Glossary ---

Metrics are exposed via the /metrics endpoint and are designed for consumption
by a monitoring system like Telegraf/VictoriaMetrics. Counters are monotonically
increasing and should be used with a rate() function.

-- Counters (events over time) --

[HSM Events]
- events_received: Total number of RESTORE events received from Redis.
  (Why it's useful: The fundamental heartbeat of the system. If this is zero,
   the daemon is not receiving any work from the HSM.)
- path_resolve_failures: Number of times 'lfs fid2path' failed.
  (Why it's useful: Indicates Lustre MDT health. A high rate suggests the MDT
   is overloaded, unresponsive, or files are being deleted very quickly.)

[Restore Activity]
- restores_triggered: Total individual 'lfs hsm_restore' commands executed.
  (Why it's useful: Shows the total volume of restore work being issued.)
- restores_skipped_dedup: Number of restores skipped because the file was
  already in the recent file_cache (to prevent re-triggering).
  (Why it's useful: A high value is normal and indicates the file cache is
   working effectively to prevent duplicate restore requests.)
- restores_completed_succeed: Number of SUCCEED events processed for in-flight files.
- restores_completed_failed: Number of FAILED events processed for in-flight files.
- restores_completed_canceled: Number of CANCELED events processed for in-flight files.
  (Why it's useful: Tracks the outcome of paced restores, providing insight into
   the health of the HSM backend.)

[Ahead-Restore & Pacing Logic]
- dirs_scanned: Number of times a directory was fully scanned to find candidate files.
  (Why it's useful: Shows how often the core ahead-restore logic is triggered.
   This should be much lower than 'events_received' due to caching.)
- dir_scans_skipped_cache: Number of scans skipped due to the dir_cache_ttl.
  (Why it's useful: A high "hit rate" is good, showing the cache is preventing
   redundant, expensive directory scans.)
- files_skipped_not_released: Files found by a scan that were skipped because they
  were not in a 'released' state (i.e., they were already online).
  (Why it's useful: A high number indicates that many files in the target
   directories are already restored, which is normal. It confirms the script
   is correctly avoiding redundant work.)
- pacing_jobs_created: Number of large restore jobs created for the Pacing Worker.
  (Why it's useful: Tracks how many large, multi-part objects are being restored.)
- pacing_jobs_completed: Number of large restore jobs that have finished entirely.
  (Why it's useful: Should track 'pacing_jobs_created' over time. If creation
   rate far exceeds completion, the system may be falling behind.)
- pacing_batches_triggered: Number of batches triggered by the Pacing Worker.
  (Why it's useful: Shows the pacing mechanism is active and breaking large
   jobs into smaller chunks.)

[Errors & Failures]
- restores_failed_runtime: 'lfs hsm_restore' command failed on execution (not timeout).
- restores_failed_timeout: 'lfs hsm_restore' command timed out.
  (Why it's useful: Direct indicator of problems with the Lustre client commands.)
- hsm_state_failed_timeout: 'lfs hsm_state' command timed out.
- hsm_state_failed_error: 'lfs hsm_state' command failed with an error.
  (Why it's useful: Direct indicator of problems with the Lustre client commands.)
- dir_scan_failures: Errors during 'os.scandir' (e.g., permission denied).
- dir_scans_aborted_limit: Scans stopped early due to 'max_files_in_dir_scan' limit.
  (Why it's useful: A safety valve; if this increments, it means you have
   directories larger than the configured limit.)
- slack_notifications_failed: Number of times sending a Slack notification failed.
  (Why it's useful: Points to issues with the Slack webhook URL, network connectivity,
  or Slack API changes.)
- pacing_jobs_deleted_stale: Number of pacing jobs automatically deleted for being
  stale (exceeding 'pacing.job_timeout').
  (Why it's useful: This is the daemon's self-healing safety net. This counter
   should ideally be zero. A non-zero value indicates a job became stuck (e.g., due
   to a lost SUCCEED event) and was forcibly cleaned up. It is a signal to check
   the application logs for warnings to identify the affected job.)
- pacing_worker_errors: Unexpected errors within the Pacing Worker main loop.
- event_processing_errors: Unexpected errors within the HSMEventConsumer main loop.
- db_cleanup_errors: Errors during the periodic database purge.
  (Why it's useful: All error counters should ideally be zero. Any increase
   points to specific, logged problems in the daemon's components.)

-- Gauges (point-in-time values) --

- pacing_jobs_active: The current number of large jobs being actively paced.
  (Why it's useful: A real-time view of the current high-level workload.)
- pacing_files_in_flight: The current number of files across all active jobs
  that have been queued for restore but have not yet completed.
  (Why it's useful: The primary indicator of load on the HSM backend. This value
   is directly controlled by the 'batch_size' and 'resume_threshold' settings.)
"""

import argparse
import yaml
import sqlite3
import subprocess
import threading
import time
import logging
import os
import re
import requests
import sys
import json
import signal
import socket
from collections import Counter
from flask import Flask, jsonify
from cachetools import TTLCache

# Assuming the stream reader library is installed
from lustre_hsm_action_stream.consumer import StreamReader

DEFAULT_CONFIG_PATH = "lrestore_ahead_client.yaml"
DEFAULT_CACHE_DB = "/var/lib/lrestore-ahead-client/lrestore_cache.db"

# --- Global objects for cross-thread access ---
metrics = None
state_lock = threading.Lock()
fid_path_cache = None # Initialized in main() from config
shutdown_event = threading.Event() # For graceful shutdown
_db_thread_local = threading.local() # For thread-safe DB connections

def load_config(config_path):
    """Loads the YAML configuration file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.critical(f"FATAL: Configuration file not found at {config_path}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"FATAL: Could not read or parse config file {config_path}: {e}")
        sys.exit(1)

def slack_notify_job_start(dirpath, files_to_restore_count, total_candidates, pacing_config, triggering_filename, slack_config, timeout_config):
    """
    Sends a formatted Slack notification for a new restore-ahead job,
    obfuscating the path if it matches a PII prefix.
    """
    webhook_url = slack_config.get("webhook_url")
    if not webhook_url:
        logging.warning("Slack notification is enabled but webhook_url is not configured.")
        return

    hostname = socket.gethostname().split('.')[0]
    activation_threshold = pacing_config.get('activation_threshold', 100)
    batch_size = pacing_config.get('batch_size', 100)

    # PII Obfuscation Logic
    dir_header = "Directory"
    display_path = dirpath # Default to the real path
    pii_prefix = slack_config.get("pii_path_prefix")

    if pii_prefix and dirpath.startswith(pii_prefix):
        logging.debug(f"PII path detected, obfuscating '{dirpath}' for Slack notification.")
        dir_header = "Directory (obfuscated)"
        try:
            path2fid_timeout = timeout_config.get('path2fid', 10)
            cmd = ["lfs", "path2fid", dirpath]
            result = subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=path2fid_timeout)

            # The output is like "[0x200004f22:0x15028:0x0]". We strip the brackets.
            fid = result.stdout.strip().strip('[]')
            if fid:
                display_path = f"FID:{fid}" # Use the FID as the display path
            else:
                display_path = "[obfuscated-pii-path-no-fid]" # Fallback if command gives empty output

        except Exception as e:
            logging.error(f"Failed to get FID for PII path '{dirpath}': {e}")
            metrics.inc("path_resolve_failures")
            display_path = "[obfuscation-failed]" # Fallback on error

    if files_to_restore_count > activation_threshold:
        text = f"`{hostname}`: Starting a restore job for *{files_to_restore_count}* files in batches of up to *{batch_size}* files."
        color = "#316cba"  # Blue for a controlled, paced job
    else:
        text = f"`{hostname}`: Restoring *{files_to_restore_count}* files"
        color = "#2eb886"  # Green for a quick, smaller job

    fallback = f"{hostname}: Starting restore-ahead for {files_to_restore_count} files in {dirpath}"

    payload = {
        "text": text,
        "channel": slack_config.get("channel"),
        "username": slack_config.get("bot_username", "Restore-Ahead Daemon"),
        "icon_emoji": slack_config.get("bot_emoji", ":rocket:"),
        "attachments": [{
            "fallback": fallback,
            "color": color,
            "fields": [
                {"title": "Triggered by", "value": f"`{triggering_filename}`", "short": True},
                {"title": "Eligible files", "value": f"{files_to_restore_count} / {total_candidates}", "short": True},
                {"title": dir_header, "value": "```" + display_path + "```", "short": False}
            ]
        }]
    }

    try:
        response = requests.post(webhook_url, data=json.dumps(payload), timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent Slack notification for job in {dirpath}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Slack notification: {e}")
        metrics.inc("slack_notifications_failed")

############ DATABASE LOGIC (THREAD-SAFE) ############
def get_db_connection(cache_db_path):
    """
    Returns a thread-local SQLite connection. Each thread (consumer, pacer)
    gets its own connection object, avoiding conflicts and the need for
    'check_same_thread=False'.
    """
    if not hasattr(_db_thread_local, 'conn'):
        # Set a timeout to prevent long lock waits from blocking a thread indefinitely
        _db_thread_local.conn = sqlite3.connect(cache_db_path, timeout=10)
    return _db_thread_local.conn

def setup_database(cache_db):
    """Initializes the SQLite database schema and indexes if they don't exist."""
    conn = get_db_connection(cache_db)
    with state_lock:
        c = conn.cursor()
        # Cache for individual files to prevent re-restoring recently handled files
        c.execute("CREATE TABLE IF NOT EXISTS restore_file_cache (path TEXT PRIMARY KEY, timestamp REAL)")
        # Cache for directories to prevent re-scanning on every event
        c.execute("CREATE TABLE IF NOT EXISTS restore_dir_cache (dirpath TEXT PRIMARY KEY, timestamp REAL)")
        # Table to track the state of large, paced restore jobs
        c.execute("""
            CREATE TABLE IF NOT EXISTS ahead_jobs (
                dirpath TEXT PRIMARY KEY,
                files_json TEXT NOT NULL,
                in_flight_files_json TEXT NOT NULL,
                next_index INTEGER NOT NULL,
                created_at REAL NOT NULL
            )
        """)

        # -- Add indexes for performance-critical queries ---
        # These indexes are crucial for the performance of the purge_old_entries function,
        # preventing it from locking the database for long periods on large tables.
        logging.info("Creating database indexes if they don't exist...")
        c.execute("CREATE INDEX IF NOT EXISTS idx_restore_file_timestamp ON restore_file_cache(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_restore_dir_timestamp ON restore_dir_cache(timestamp)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ahead_jobs_created_at ON ahead_jobs(created_at)")

        conn.commit()

def should_restore_file(cache_db, path, ttl):
    """Checks if a file should be restored based on the file cache TTL."""
    conn = get_db_connection(cache_db)
    with state_lock:
        c = conn.cursor()
        c.execute("SELECT timestamp FROM restore_file_cache WHERE path=?", (path,))
        row = c.fetchone()
    now = time.time()
    return row is None or (now - row[0]) > ttl

def mark_file_restored(cache_db, path):
    """Marks a file as restored (or queued for restore) in the file cache."""
    conn = get_db_connection(cache_db)
    with state_lock:
        now = time.time()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO restore_file_cache (path, timestamp) VALUES (?, ?)", (path, now))
        conn.commit()

def should_scan_dir(cache_db, dirpath, ttl):
    """
    Checks if a directory should be scanned for a new ahead-restore job.
    Returns False if a job is already active or if the dir is in the temporary cache.
    """
    conn = get_db_connection(cache_db)
    with state_lock:
        c = conn.cursor()
        # First, check if a job is already active for this directory.
        c.execute("SELECT 1 FROM ahead_jobs WHERE dirpath=?", (dirpath,))
        if c.fetchone():
            logging.debug(f"Dir {dirpath} has an active pacing job, skipping scan.")
            return False

        # Second, check if the directory is in the temporary scan cache.
        c.execute("SELECT timestamp FROM restore_dir_cache WHERE dirpath=?", (dirpath,))
        row = c.fetchone()

    now = time.time()
    if row is None or (now - row[0]) > ttl:
        return True
    else:
        # Explicitly log when skipping due to the cache TTL.
        remaining_ttl = int(ttl - (now - row[0]))
        logging.debug(f"Dir {dirpath} is in scan cache, skipping scan. (TTL remaining: {remaining_ttl}s)")
        metrics.inc("dir_scans_skipped_cache")
        return False

def mark_dir_scanned(cache_db, dirpath):
    """Marks a directory as recently scanned in the directory cache."""
    conn = get_db_connection(cache_db)
    with state_lock:
        now = time.time()
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO restore_dir_cache (dirpath, timestamp) VALUES (?, ?)", (dirpath, now))
        conn.commit()

def purge_old_entries(cache_db, file_ttl, dir_ttl):
    """Periodically cleans up expired entries from the cache tables."""
    conn = get_db_connection(cache_db)
    with state_lock:
        now = time.time()
        c = conn.cursor()
        logging.info("Purging old entries from restore_file_cache and restore_dir_cache...")
        c.execute("DELETE FROM restore_file_cache WHERE ? - timestamp > ?", (now, file_ttl))
        c.execute("DELETE FROM restore_dir_cache WHERE ? - timestamp > ?", (now, dir_ttl))
        # Also purge completed jobs older than the dir_ttl to prevent stale entries
        # A job is "completed" if next_index >= total files and in_flight is empty.
        # This is handled in the pacer, but we add a safety net here for jobs older than dir_ttl.
        c.execute("DELETE FROM ahead_jobs WHERE ? - created_at > ?", (now, dir_ttl * 2)) # Keep jobs for 2x TTL
        conn.commit()
        logging.info("Vacuuming database to reclaim space...")
        c.execute("VACUUM")
    logging.info("Bulk cache purge complete.")

############ RESTORE & FID LOGIC ############
def fid_to_path(fid, mountpoint, timeout):
    """
    Resolves a Lustre FID to a path, using a time-aware in-memory cache.
    NOTE: cachetools.TTLCache is not thread-safe. However, in this application,
    this function is ONLY ever called by the single HSMEventConsumer thread,
    so no external lock is required.
    """
    if fid in fid_path_cache:
        return fid_path_cache[fid]
    try:
        cmd = ["lfs", "fid2path", mountpoint, fid]
        result = subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=timeout)
        path = result.stdout.strip() or None
        if path:
            fid_path_cache[fid] = path
        return path
    except Exception as e:
        logging.error(f"fid2path failed for FID {fid} (cmd: {' '.join(cmd)}): {e}")
        return None

def filter_released_files(paths, restore_logic_config, timeout):
    """
    Given a list of file paths, filters them to return only those that are
    in a 'released' state and match the target archive ID.
    Uses 'lfs hsm_state' in batches for efficiency.
    """
    if not paths:
        return []

    target_archive_id = restore_logic_config.get('target_archive_id', 1)
    chunk_size = 100  # Process 100 files at a time to avoid arg-list-too-long errors
    released_paths = []

    for i in range(0, len(paths), chunk_size):
        if shutdown_event.is_set(): break

        chunk = paths[i:i + chunk_size]
        try:
            cmd = ["lfs", "hsm_state"] + chunk
            result = subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=timeout)

            for line in result.stdout.strip().split('\n'):
                if not line: continue

                parts = line.split(':', 1)
                if len(parts) < 2: continue
                path, state_str = parts

                # Check for required state flags
                is_released = 'released' in state_str
                is_archived = 'exists' in state_str and 'archived' in state_str
                is_not_lost = 'lost' not in state_str

                # Check archive ID
                archive_match = re.search(r'archive_id:(\d+)', state_str)
                archive_id_ok = archive_match and int(archive_match.group(1)) == target_archive_id

                if is_released and is_archived and is_not_lost and archive_id_ok:
                    released_paths.append(path.strip())

        except subprocess.TimeoutExpired:
            logging.error(f"Command 'lfs hsm_state' timed out after {timeout}s for a batch of files. Skipping batch.")
            metrics.inc("hsm_state_failed_timeout")
        except Exception as e:
            logging.error(f"Batch 'lfs hsm_state' failed: {e}. Skipping batch.")
            metrics.inc("hsm_state_failed_error")

    skipped_count = len(paths) - len(released_paths)
    if skipped_count > 0:
        metrics.inc("files_skipped_not_released", skipped_count)

    return released_paths

def trigger_restore_batch(paths, dry_run, cache_db, file_ttl, timeout):
    """
    Triggers 'lfs hsm_restore' for a list of paths.
    Uses subprocess.run() to prevent zombie processes.
    NOTE: On a trigger failure (timeout or other exception), the file is logged
    and skipped. It is NOT automatically retried by this function.
    """
    triggered_paths = []
    for path in paths:
        if shutdown_event.is_set():
            logging.info("Shutdown signaled, aborting restore batch.")
            break
        if not should_restore_file(cache_db, path, file_ttl):
            metrics.inc("restores_skipped_dedup")
            continue
        if dry_run:
            mark_file_restored(cache_db, path)
            logging.info(f"DRY-RUN: Would restore {path}")
            triggered_paths.append(path)
        else:
            try:
                cmd = ["lfs", "hsm_restore", path]
                subprocess.run(cmd, check=True, timeout=timeout)
                mark_file_restored(cache_db, path)
                logging.info(f"Triggered restore: {path}")
                triggered_paths.append(path)
            except subprocess.TimeoutExpired:
                logging.error(f"Command '{' '.join(cmd)}' timed out after {timeout}s. File will be skipped.")
                metrics.inc("restores_failed_timeout")
            except Exception as e:
                logging.error(f"Restore failed for {path}: {e}. File will be skipped.")
                metrics.inc("restores_failed_runtime")
    if triggered_paths:
        metrics.inc("restores_triggered", len(triggered_paths))
    return triggered_paths

############ METRICS & HTTP API ############
class MetricsTracker:
    """A thread-safe class for aggregating metrics for the API endpoint."""
    def __init__(self, fsname):
        self.fsname = fsname
        self.lock = threading.Lock()
        self.counters = Counter()
        self.gauges = {}

    def inc(self, metric_name, value=1):
        with self.lock:
            self.counters[metric_name] += value

    def set_gauge(self, metric_name, value):
        with self.lock:
            self.gauges[metric_name] = value

    def get_metrics(self):
        """Returns all current metric values. Counters are monotonic and never reset."""
        with self.lock:
            output = {
                "fsname": self.fsname,
                "counters": dict(self.counters),
                "gauges": dict(self.gauges)
            }
        return output

app = Flask(__name__)
@app.route('/metrics')
def get_metrics_endpoint():
    """Endpoint for Telegraf to scrape structured metrics."""
    return jsonify(metrics.get_metrics())

@app.route('/health')
def get_health_endpoint():
    """Basic health check for load balancers or service monitoring."""
    return jsonify({"status": "ok", "fsname": metrics.fsname})

############ PACING WORKER ############
def pacing_worker_thread(config, cache_db, dry_run):
    """
    A worker thread that periodically checks the state of active pacing jobs
    and queues new batches of restores when capacity is available.
    """
    pacing_conf = config['pacing']
    timeouts = config['timeouts']
    check_interval = pacing_conf['check_interval']
    resume_threshold = pacing_conf['resume_threshold']
    batch_size = pacing_conf['batch_size']
    job_timeout = pacing_conf.get('job_timeout', 86400)
    file_ttl = config.get("file_cache_ttl", 600)
    restore_timeout = timeouts['hsm_restore']

    logging.info(
        f"Pacing worker started. Check interval: {check_interval}s, "
        f"Resume Threshold: {resume_threshold}, Stale Job Timeout: {job_timeout}s."
    )

    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=check_interval)
        if shutdown_event.is_set(): break
        logging.debug("Pacing worker running check...")

        conn = get_db_connection(cache_db)
        with state_lock:
            c = conn.cursor()
            c.execute("SELECT dirpath FROM ahead_jobs")
            job_dirpaths = [row[0] for row in c.fetchall()]

        active_job_count = len(job_dirpaths)
        total_in_flight = 0

        for dirpath in job_dirpaths:
            if shutdown_event.is_set(): break

            job_data_to_process = None
            with state_lock:
                c = conn.cursor()
                # Also fetch the creation time for the timeout check
                c.execute("SELECT files_json, in_flight_files_json, next_index, created_at FROM ahead_jobs WHERE dirpath=?", (dirpath,))
                if job_data := c.fetchone():
                    files_json, in_flight_json, next_index, created_at = job_data

                    # --- Stale Job Check ---
                    if (time.time() - created_at) > job_timeout:
                        logging.warning(f"Pacing job for {dirpath} has been active for more than {job_timeout}s. Deleting as stale.")
                        c.execute("DELETE FROM ahead_jobs WHERE dirpath=?", (dirpath,))
                        conn.commit()
                        metrics.inc("pacing_jobs_deleted_stale")
                        continue
                    # --- End Stale Job Check ---

                    all_files = json.loads(files_json)
                    in_flight_files = json.loads(in_flight_json)
                    total_in_flight += len(in_flight_files)

                    if next_index >= len(all_files) and not in_flight_files:
                        logging.info(f"Pacing job for {dirpath} is complete. Deleting.")
                        c.execute("DELETE FROM ahead_jobs WHERE dirpath=?", (dirpath,))
                        conn.commit()
                        metrics.inc("pacing_jobs_completed")
                        continue

                    if len(in_flight_files) < resume_threshold and (len(all_files) - next_index) > 0:
                        job_data_to_process = {
                            "dirpath": dirpath,
                            "files_to_trigger": all_files[next_index : next_index + batch_size],
                            "next_index": next_index
                        }

            if job_data_to_process:
                logging.info(f"Job {dirpath}: in-flight count below threshold. Triggering next batch.")
                triggered_paths = []
                try:
                    triggered_paths = trigger_restore_batch(
                        job_data_to_process["files_to_trigger"],
                        dry_run, cache_db, file_ttl, restore_timeout
                    )
                except Exception as e:
                    logging.error(f"Error in trigger_restore_batch for {dirpath}: {e}", exc_info=True)
                    metrics.inc("pacing_worker_errors")

                if triggered_paths:
                    with state_lock:
                        c = conn.cursor()
                        c.execute("SELECT in_flight_files_json FROM ahead_jobs WHERE dirpath=?", (dirpath,))
                        if current_job_data := c.fetchone():
                            # DOC: The logic below assumes this PacingWorker is the sole producer
                            # of in_flight_files, and the HSMEventConsumer is the sole consumer.
                            # This makes this append-only operation safe.
                            current_in_flight = json.loads(current_job_data[0])
                            new_in_flight_files = current_in_flight + triggered_paths
                            new_next_index = job_data_to_process["next_index"] + len(job_data_to_process["files_to_trigger"])

                            c.execute("UPDATE ahead_jobs SET next_index = ?, in_flight_files_json = ? WHERE dirpath = ?",
                                      (new_next_index, json.dumps(new_in_flight_files), dirpath))
                            conn.commit()
                            metrics.inc("pacing_batches_triggered")

        metrics.set_gauge("pacing_jobs_active", active_job_count)
        metrics.set_gauge("pacing_files_in_flight", total_in_flight)
    logging.info("Pacing worker shut down.")

############ DB CLEANUP WORKER ############
def db_cleanup_thread(cache_db, config):
    """
    A simple worker thread that periodically purges old entries from the DB.
    """
    file_ttl = config.get("file_cache_ttl", 600)
    dir_ttl = config.get("dir_cache_ttl", 21600)
    # Cleanup interval should be reasonably long, e.g., once an hour
    cleanup_interval = config.get("db_cleanup_interval", 3600)
    logging.info(f"DB Cleanup worker started. Interval: {cleanup_interval}s.")

    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=cleanup_interval)
        if shutdown_event.is_set(): break

        try:
            purge_old_entries(cache_db, file_ttl, dir_ttl)
        except Exception as e:
            logging.error(f"Error during DB cleanup: {e}", exc_info=True)
            metrics.inc("db_cleanup_errors")

    logging.info("DB Cleanup worker shut down.")

############ MAIN EVENT CONSUMER ############
def event_consumer_thread(config, cache_db, dry_run):
    """
    The main event loop. Listens to Redis streams and dispatches actions:
    - On new RESTORE events, it triggers scans.
    - On SUCCEED/FAILED events, it updates the state of paced jobs.
    """
    fs_mountpoint = config["fs_mountpoint"]
    restore_regex = re.compile(config.get("restore_regex", "^.*$"))
    file_ttl = config.get("file_cache_ttl", 600)
    dir_ttl = config.get("dir_cache_ttl", 21600)
    pacing_config = config.get('pacing', {})
    pacing_threshold = pacing_config.get('activation_threshold', 100)
    max_scan = config['limits']['max_files_in_dir_scan']
    fid_timeout = config['timeouts']['fid2path']
    hsm_restore_timeout = config['timeouts']['hsm_restore']
    hsm_state_timeout = config['timeouts']['hsm_state']
    slack_config = config.get('slack_notifications', {})
    slack_enabled = slack_config.get('enabled', False)

    try:
        reader = StreamReader(
            host=config['redis_host'], port=config['redis_port'],
            db=config['redis_db'], prefix=config['redis_stream_prefix']
        )
        reader.ping() if hasattr(reader, 'ping') else None
    except Exception as e:
        logging.critical(f"FATAL: Cannot connect to Redis: {e}")
        sys.exit(1)

    logging.info("HSM Event Consumer started.")

    while not shutdown_event.is_set():
        try:
            for event in reader.events(from_beginning=False, block_ms=5000):
                if shutdown_event.is_set(): break
                if not event: continue

                e_data = event.data
                if e_data.get("action") != "RESTORE" or not e_data.get("fid"):
                    continue
                metrics.inc("events_received")

                fid, status = e_data.get("fid"), e_data.get("status")
                path = fid_to_path(fid, fs_mountpoint, fid_timeout)
                if not path:
                    metrics.inc("path_resolve_failures")
                    continue

                # --- Handler for COMPLETED restores (for pacing feedback) ---
                if status in ("SUCCEED", "FAILED", "CANCELED"):
                    dirpath = os.path.dirname(path)
                    conn = get_db_connection(cache_db)
                    with state_lock:
                        c = conn.cursor()
                        c.execute("SELECT in_flight_files_json FROM ahead_jobs WHERE dirpath=?", (dirpath,))
                        if job_row := c.fetchone():
                            in_flight_files = json.loads(job_row[0])
                            if path in in_flight_files:
                                in_flight_files.remove(path)
                                c.execute("UPDATE ahead_jobs SET in_flight_files_json = ? WHERE dirpath = ?", (json.dumps(in_flight_files), dirpath))
                                conn.commit()
                                metrics.inc(f"restores_completed_{status.lower()}")
                    continue # This event's work is done.

                # --- Handler for NEW restores (triggers scans) ---
                # Immediately mark the triggering file as "handled" in the file cache.
                # This prevents our own restore logic from re-triggering a restore for
                # the very file that initiated the process.
                mark_file_restored(cache_db, path)
                dirpath = os.path.dirname(path)
                if should_scan_dir(cache_db, dirpath, dir_ttl):
                    logging.info(f"New restore in {dirpath} triggers ahead-restore scan.")
                    metrics.inc("dirs_scanned")

                    try:
                        # Step 1: Gather all candidate files matching the regex.
                        candidate_files = []
                        with os.scandir(dirpath) as it:
                            for entry in it:
                                if len(candidate_files) >= max_scan:
                                    logging.warning(f"Directory {dirpath} has more than {max_scan} files. Aborting scan to prevent OOM.")
                                    metrics.inc("dir_scans_aborted_limit")
                                    candidate_files = [] # Ensure we don't process a partial list
                                    break
                                if entry.is_file() and restore_regex.match(entry.name):
                                    candidate_files.append(entry.path)

                        # Step 2: Filter the candidates to find only those that are actually released.
                        if candidate_files:
                            logging.debug(f"Found {len(candidate_files)} candidate files in {dirpath}. Checking HSM state...")
                            files_to_restore = filter_released_files(candidate_files, config['restore_logic'], hsm_state_timeout)
                            # The `path` variable still holds the full path of the file that
                            # triggered this entire process. We must remove it from our
                            # restore-ahead list to avoid re-triggering it.
                            if path in files_to_restore:
                                files_to_restore.remove(path)
                                logging.debug(f"Removed triggering file '{os.path.basename(path)}' from restore-ahead list.")
                            logging.info(f"Found {len(files_to_restore)} released files to restore in {dirpath}.")
                        else:
                            files_to_restore = []

                        # Step 3: Process the filtered list.
                        if files_to_restore:
                            files_to_restore.sort()

                            # Send SLACK notification
                            if slack_enabled:
                                slack_notify_job_start(
                                    dirpath,
                                    len(files_to_restore),
                                    len(candidate_files),
                                    pacing_config,
                                    os.path.basename(path),
                                    slack_config,
                                    config['timeouts'])

                            if len(files_to_restore) > pacing_threshold:
                                conn = get_db_connection(cache_db)
                                with state_lock:
                                    c = conn.cursor()
                                    c.execute("INSERT OR IGNORE INTO ahead_jobs (dirpath, files_json, in_flight_files_json, next_index, created_at) VALUES (?, ?, ?, ?, ?)",
                                              (dirpath, json.dumps(files_to_restore), json.dumps([]), 0, time.time()))
                                    conn.commit()
                                metrics.inc("pacing_jobs_created")
                            else:
                                trigger_restore_batch(files_to_restore, dry_run, cache_db, file_ttl, hsm_restore_timeout)

                    except (FileNotFoundError, PermissionError) as e:
                        logging.warning(f"Cannot scan directory {dirpath}: {e}")
                        metrics.inc("dir_scan_failures")

                    mark_dir_scanned(cache_db, dirpath)
        except Exception as e:
            if not shutdown_event.is_set():
                logging.error(f"Unhandled error in event consumer loop: {e}", exc_info=True)
                metrics.inc("event_processing_errors")
                time.sleep(5)
    logging.info("Event consumer shut down.")

def main():
    parser = argparse.ArgumentParser(
        description="Lustre HSM restore-ahead client daemon.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-c", "--config", default=DEFAULT_CONFIG_PATH, help="Path to YAML config file.")
    parser.add_argument("--cache-db", default=DEFAULT_CACHE_DB, help="Path to SQLite DB for cache and job state.")
    parser.add_argument("--dry-run", action="store_true", help="Log actions instead of executing them; does not trigger restores.")
    parser.add_argument("-l", "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level.")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format='%(asctime)s %(levelname)-8s %(threadName)s: %(message)s')

    config = load_config(args.config)
    logging.debug(f"Loaded configuration: {json.dumps(config, indent=2)}")
    fsname = config.get("fsname")
    if not fsname:
        sys.exit("FATAL: 'fsname' must be defined in the configuration file.")

    # --- Configuration validation ---
    pacing_conf = config.get('pacing', {})
    batch_size = pacing_conf.get('batch_size')
    resume_threshold = pacing_conf.get('resume_threshold')
    if batch_size is not None and resume_threshold is not None:
        if resume_threshold >= batch_size:
            logging.critical(
                f"FATAL: Configuration error in [pacing]: 'resume_threshold' ({resume_threshold}) "
                f"must be less than 'batch_size' ({batch_size}) to prevent deadlocks."
            )
            sys.exit(1)
    # --- End validation ---

    global metrics, fid_path_cache
    metrics = MetricsTracker(fsname)
    cache_conf = config.get('cache', {})
    fid_path_cache = TTLCache(maxsize=cache_conf.get('fid_path_cache_size', 50000), ttl=cache_conf.get('fid_path_cache_ttl', 900))

    db_dir = os.path.dirname(args.cache_db)
    try:
        ### Rely solely on makedirs's idempotency to prevent a TOCTOU race condition.
        os.makedirs(db_dir, exist_ok=True)
        logging.info(f"Created/verified database directory: {db_dir}")
    except PermissionError as e:
        logging.critical(f"FATAL: Cannot create database directory {db_dir}: {e}")
        sys.exit(1)

    setup_database(args.cache_db)
    logging.info(f"Using database at {args.cache_db}")

    def signal_handler(signum, frame):
        if not shutdown_event.is_set():
            logging.warning(f"Shutdown signal {signum} received. Stopping threads gracefully...")
            shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    threads = [
        threading.Thread(target=event_consumer_thread, args=(config, args.cache_db, args.dry_run), name="HSMEventConsumer"),
        threading.Thread(target=pacing_worker_thread, args=(config, args.cache_db, args.dry_run), name="PacingWorker"),
        threading.Thread(target=db_cleanup_thread, args=(args.cache_db, config), name="DBCleanupWorker"),
        # For production, this should be run via a proper WSGI server like gunicorn.
        # Running it in a thread is acceptable for internal, low-traffic tools.
        # Making it a daemon thread ensures it won't block the main process from exiting.
        threading.Thread(target=lambda: app.run(host=config['api_host'], port=config['api_port']), name="APIServer", daemon=True)
    ]
    for t in threads:
        t.start()

    logging.info(f"All services started for filesystem '{fsname}'. Waiting for shutdown signal.")

    # Main thread waits for non-daemon threads to finish
    for t in threads:
        if not t.isDaemon():
            t.join()

    logging.info("All threads have been shut down. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    main()
