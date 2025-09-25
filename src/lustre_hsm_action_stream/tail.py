# Copyright (C) 2025
#      The Board of Trustees of the Leland Stanford Junior University
# Written by Stephane Thiell <sthiell@stanford.edu>
#
# Licensed under GPL v3 (see https://www.gnu.org/licenses/).
"""
hsm-stream-tail

A command-line utility that "tails" the Lustre HSM action stream, providing a
real-time, human-readable log of events as they occur. It resolves FIDs to
paths for easy identification of affected files.
"""

import sys
import os
import signal
import shutil
import yaml
import argparse
import subprocess
import logging
from datetime import datetime

# Import the new high-level consumer API
from .consumer import StreamReader

# --- Terminal Formatting ---

# ANSI color codes for terminal output
COLORS = {
    'GREEN': '\033[92m', 'YELLOW': '\033[93m', 'RED': '\033[91m',
    'BLUE': '\033[94m', 'GRAY': '\033[90m', 'DIM': '\033[2m', 'ENDC': '\033[0m'
}
STATUS_COLORS = {
    'STARTED': COLORS['BLUE'], 'WAITING': COLORS['YELLOW'], 'SUCCEED': COLORS['GREEN'],
    'FAILED': COLORS['RED'], 'CANCELED': COLORS['RED'], 'PURGED': COLORS['DIM'],
}
# Only output colors to a real terminal, unless NO_COLOR is set
USE_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

DEFAULT_CONFIG_PATH = "/etc/lustre-hsm-action-stream/hsm_stream_tail.yaml"

def colorize(text, color_code):
    """Wraps text in ANSI color codes if enabled."""
    return f"{color_code}{text}{COLORS['ENDC']}" if USE_COLOR and color_code else text

# --- Helper Functions ---

def load_config(path):
    """Loads YAML config. Returns an empty dict if file is not found or invalid."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Warning: Could not load or parse config file {path}: {e}", file=sys.stderr)
        return {}

def resolve_fid_to_path(mountpoint, fid, fid_cache):
    """
    Resolves a Lustre FID to path using 'lfs fid2path', caching results.
    Returns the path as a string, or None if resolution fails.
    """
    if not fid:
        return None
    if fid in fid_cache:
        return fid_cache[fid]
    try:
        cmd = ['lfs', 'fid2path', mountpoint, fid]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=10)
        if proc.returncode == 0 and proc.stdout:
            path = proc.stdout.strip().split('\n')[0]
            fid_cache[fid] = path
            return path
        return None
    except Exception as e:
        logging.warning(f"FID resolution failed for {fid}: {e}")
        return None

# --- Main Application Logic ---

def tail_events(conf, from_beginning, hidden_items):
    """
    Connects to Redis via StreamReader and continuously prints formatted events.
    """
    # Create the reader instance. It handles connection, discovery, and reading.
    reader = StreamReader(
        host=conf['redis_host'],
        port=conf['redis_port'],
        db=conf['redis_db'],
        prefix=conf['redis_stream_prefix']
    )

    # Print a user-friendly startup message to stderr, as stdout is for event data.
    print(f"Tailing streams with prefix '{conf['redis_stream_prefix']}:*'. Press Ctrl+C to exit.", file=sys.stderr)
    # Log the same info for consistency, which respects the chosen log level.
    logging.info(f"Tailing streams with prefix '{conf['redis_stream_prefix']}:*'.")
    fid_cache = {}

    try:
        # The main loop is now a simple, elegant for-loop over the event generator.
        for event in reader.events(from_beginning=from_beginning, block_ms=0):
            if not event: continue

            # `event` is a StreamEvent namedtuple(stream, id, data)
            e_data = event.data

            # --- Filtering Logic ---
            event_type = e_data.get('event_type')
            action = e_data.get('action')
            status = e_data.get('status')
            filterable_action = action or event_type

            if filterable_action in hidden_items or status in hidden_items:
                continue

            # --- Formatting and Printing Logic ---
            ts_val = e_data.get('timestamp')
            ts = datetime.fromtimestamp(ts_val).strftime('%Y-%m-%d %H:%M:%S') if ts_val else 'N/A'
            mdt = e_data.get('mdt', '?')
            fid = e_data.get('fid')

            color = STATUS_COLORS.get(status, COLORS['ENDC'])
            status_str = colorize(f"{status or '?': <8}", color)

            path_str = ""
            if fid:
                path = resolve_fid_to_path(conf['mountpoint'], fid, fid_cache)
                path_str = f"-> {path}" if path else f"-> (path for {fid} not found)"

            # Print to stdout, which is the primary purpose of this tool.
            print(f"{ts} [{mdt}] {filterable_action or '?': <8} {status_str} {path_str} {colorize(f'(id: {event.id})', COLORS['DIM'])}")

    except KeyboardInterrupt:
        print("\nExiting.", file=sys.stderr)
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

def main():
    """Main entry point for the hsm-stream-tail executable."""
    parser = argparse.ArgumentParser(
        description="Tail the Lustre HSM action stream. By default, events from PURGED actions are hidden.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    config_group = parser.add_argument_group('Configuration')
    config_group.add_argument('-c', '--config', default=DEFAULT_CONFIG_PATH, help="Path to YAML config file.")
    config_group.add_argument('--mountpoint', default=None, help="Lustre mountpoint for 'lfs fid2path' (overrides config).")
    config_group.add_argument('--host', default=None, help="Redis server host (overrides config).")
    config_group.add_argument('--port', type=int, default=None, help="Redis server port (overrides config).")
    config_group.add_argument('--db', type=int, default=None, help="Redis database number (overrides config).")
    config_group.add_argument('--stream-prefix', default=None, help="Prefix of Redis Streams (overrides config).")
    config_group.add_argument('--from-beginning', action='store_true', help="Start tailing from the beginning of all streams.")
    config_group.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the logging level for stderr output.")
    config_group.add_argument("--log-file", help="Redirect logging output to a file instead of stderr.")

    filter_group = parser.add_argument_group('Filtering Options', 'Modify the default view (which hides PURGED).')
    filter_group.add_argument('--show', nargs='+', metavar='ITEM', help="Show specific action/status types hidden by default (e.g., --show PURGED).")
    filter_group.add_argument('--hide', nargs='+', metavar='ITEM', help="Hide additional action/status types (e.g., --hide STARTED).")

    args = parser.parse_args()

    config_file_data = load_config(args.config)
    conf = {
        'mountpoint': args.mountpoint or config_file_data.get('mountpoint'),
        'redis_host': args.host or config_file_data.get('redis_host', 'localhost'),
        'redis_port': args.port if args.port is not None else config_file_data.get('redis_port', 6379),
        'redis_db': args.db if args.db is not None else config_file_data.get('redis_db', 1),
        'redis_stream_prefix': args.stream_prefix or config_file_data.get('redis_stream_prefix', 'hsm:actions')
    }

    log_kwargs = {"level": args.log_level.upper(), "format": "%(asctime)s %(levelname)s:%(name)s:%(message)s"}
    if args.log_file:
        log_kwargs["filename"] = args.log_file
    else:
        log_kwargs["stream"] = sys.stderr
    logging.basicConfig(**log_kwargs)

    # Start with the default set of items to hide
    hidden_items = {'PURGED'}
    if args.show:
        hidden_items.difference_update(item.upper() for item in args.show)
    if args.hide:
        hidden_items.update(item.upper() for item in args.hide)

    if not conf['mountpoint']: parser.error("--mountpoint must be specified via command line or config file.")
    if shutil.which('lfs') is None:
        logging.critical("'lfs' command not found. Is Lustre client installed and in your PATH?")
        sys.exit(1)

    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    tail_events(conf, args.from_beginning, hidden_items)
    sys.exit(0)

if __name__ == "__main__":
    main()
