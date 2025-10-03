# tests/conftest.py

import pytest
import os
import sys
from unittest.mock import patch
import redis

# --- Add src to path to allow imports ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from lustre_hsm_action_stream.shipper import main as shipper_main

@pytest.fixture
def redis_conn_params():
    """
    Fixture to provide connection details for a live Redis instance.
    It now uses the redis library to ping and flush the test DB.
    """
    params = {"host": "localhost", "port": 6379, "db": 2}

    try:
        r = redis.Redis(**params)
        r.ping()
        r.flushdb()
    except redis.exceptions.ConnectionError as e:
        pytest.skip(f"Could not connect to Redis for testing. Is Redis running? Error: {e}")

    yield params

@pytest.fixture
def test_env(tmp_path, redis_conn_params):
    """Creates a temporary test environment with configs and mock Lustre paths."""
    testfs_path = tmp_path / "sys/kernel/debug/lustre/mdt/testfs-MDT0000"
    actions_file = testfs_path / "hsm" / "actions"
    os.makedirs(actions_file.parent)
    actions_file.touch()

    # --- Shipper Config ---
    shipper_config_path = tmp_path / "shipper.yaml"
    shipper_config_path.write_text(f"""
redis_host: "{redis_conn_params['host']}"
redis_port: {redis_conn_params['port']}
redis_db: {redis_conn_params['db']}
redis_stream_prefix: "hsm:actions"
mdt_watch_glob: "{actions_file}"
poll_interval: 1
reconcile_interval: 3 # Short interval for testing
trim_chunk_size: 1000 # Use new key name
use_approximate_trimming: false # Use exact trimming for predictable tests
cache_path: "{tmp_path}/shipper_cache.json"
log_level: "DEBUG"
""")

    # --- Consumer Configs (for stats, tail) ---
    stats_config_path = tmp_path / "stats.yaml"
    stats_config_path.write_text(f"""
redis_host: "{redis_conn_params['host']}"
redis_port: {redis_conn_params['port']}
redis_db: {redis_conn_params['db']}
redis_stream_prefix: "hsm:actions"
""")

    return {
        "actions_file": actions_file,
        "shipper_config": shipper_config_path,
        "stats_config": stats_config_path,
        "mdt_name": "testfs-MDT0000",
        "stream_name": f"hsm:actions:{'testfs-MDT0000'}"
    }

@pytest.fixture
def run_cli():
    """Returns a helper function to run a main function from one of the tools."""
    def _run_cli_wrapper(main_func, *args):
        final_args = list(args)
        if main_func != shipper_main and '--run-once' not in final_args:
            # Check if the tool supports --run-once before adding it
            if main_func.__name__ in ['viewer_main', 'tail_main', 'stats_main']:
                 # Some tools might not have it, let's assume these do for now
                 pass # The logic is now inside the tests themselves

        with patch.object(sys, 'argv', [main_func.__module__] + final_args):
            try:
                main_func()
            except SystemExit as e:
                if e.code != 0:
                    raise
    return _run_cli_wrapper
