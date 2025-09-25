# tests/test_advanced.py

import pytest
import os
import sys
import json
import logging # Import logging to set the level for caplog
from unittest.mock import patch
import redis

# --- Imports from the application ---
from lustre_hsm_action_stream.parser import parse_action_line
from lustre_hsm_action_stream.shipper import main as shipper_main
from lustre_hsm_action_stream.stats import main as stats_main

# =============================================================================
#  1. Unit Tests for the Parser
# =============================================================================

@pytest.mark.parametrize("line, expected", [
    (
        "lrh=[type=10680000 len=192 idx=517/31144] fid=[0x2800059ca:0xc464:0x0] action=ARCHIVE status=WAITING data=[...]",
        {'cat_idx': 517, 'rec_idx': 31144, 'fid': '0x2800059ca:0xc464:0x0', 'action': 'ARCHIVE', 'status': 'WAITING'}
    ),
    (
        "idx=[1/2] action=RESTORE fid=[0xabc] status=STARTED",
        {'cat_idx': 1, 'rec_idx': 2, 'fid': '0xabc', 'action': 'RESTORE', 'status': 'STARTED'}
    ),
    ("idx=[1] action=ARCHIVE fid=[0x1] status=STARTED", None),
    ("", None),
    ("some other kernel message", None)
])
def test_parse_action_line(line, expected):
    """
    Tests the parser with various valid and invalid hsm/actions log lines.
    This is a pure unit test and does not require Redis or a filesystem.
    """
    result = parse_action_line(line)
    if expected is None:
        assert result is None
    else:
        assert result is not None
        for key, value in expected.items():
            assert key in result
            assert result[key] == value


# =============================================================================
#  2. Integration Tests for Multi-MDT Support
# =============================================================================

@pytest.fixture
def multi_mdt_test_env(tmp_path, redis_conn_params):
    """Fixture to create a mock environment with two MDTs."""
    mdt0_path = tmp_path / "sys/kernel/debug/lustre/mdt/testfs-MDT0000/hsm/actions"
    os.makedirs(mdt0_path.parent)
    mdt0_path.touch()
    mdt1_path = tmp_path / "sys/kernel/debug/lustre/mdt/testfs-MDT0001/hsm/actions"
    os.makedirs(mdt1_path.parent)
    mdt1_path.touch()
    shipper_config_path = tmp_path / "shipper.yaml"
    shipper_config_path.write_text(f"""
redis_host: "{redis_conn_params['host']}"
redis_port: {redis_conn_params['port']}
redis_db: {redis_conn_params['db']}
redis_stream_prefix: "hsm:actions"
mdt_watch_glob: "{tmp_path}/sys/kernel/debug/lustre/mdt/testfs-MDT*/hsm/actions"
poll_interval: 1
reconcile_interval: 3600
aggressive_trim_threshold: 5000
cache_path: "{tmp_path}/shipper_cache.json"
log_level: "DEBUG"
""")
    stats_config_path = tmp_path / "stats.yaml"
    stats_config_path.write_text(f"""
redis_host: "{redis_conn_params['host']}"
redis_port: {redis_conn_params['port']}
redis_db: {redis_conn_params['db']}
redis_stream_prefix: "hsm:actions"
""")
    return {
        "mdt0_actions_file": mdt0_path,
        "mdt1_actions_file": mdt1_path,
        "shipper_config": shipper_config_path,
        "stats_config": stats_config_path,
        "stream_name_0": "hsm:actions:testfs-MDT0000",
        "stream_name_1": "hsm:actions:testfs-MDT0001"
    }

def test_multi_mdt_shipper_and_stats(multi_mdt_test_env, redis_conn_params, capsys, run_cli):
    env = multi_mdt_test_env
    r = redis.Redis(**redis_conn_params, decode_responses=True)
    env["mdt0_actions_file"].write_text("idx=[0/1] action=ARCHIVE status=STARTED\n")
    env["mdt1_actions_file"].write_text("idx=[1/1] action=RESTORE status=WAITING\nidx=[1/2] action=RESTORE status=STARTED\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]))
    assert r.xlen(env["stream_name_0"]) == 1
    assert r.xlen(env["stream_name_1"]) == 2
    run_cli(stats_main, '-c', str(env["stats_config"]))
    stats_json = json.loads(capsys.readouterr().out)
    assert stats_json['summary']['total_live_actions'] == 3
    assert stats_json['summary']['total_events_replayed'] == 3
    mdts_in_breakdown = {item['mdt'] for item in stats_json['breakdown']}
    assert mdts_in_breakdown == {'testfs-MDT0000', 'testfs-MDT0001'}


# =============================================================================
#  3. Test for Consumer Robustness to Bad Data
# =============================================================================

def test_consumer_robustness_on_bad_data(test_env, redis_conn_params, caplog, capsys, run_cli):
    """
    Tests that consumer tools (like hsm-stream-stats) do not crash when
    encountering malformed data in a Redis stream, using caplog to check logs.
    """
    env = test_env
    r = redis.Redis(**redis_conn_params)

    # 1. Use the shipper to add one valid event.
    env["actions_file"].write_text("idx=[0/1] action=ARCHIVE status=STARTED\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]))

    # 2. Manually inject malformed events into the stream.
    r.xadd(env["stream_name"], {"data": b"this is not json"})
    r.xadd(env["stream_name"], {"data": json.dumps({"mdt": "testfs-MDT0000", "cat_idx": 1, "rec_idx": 1})})

    # 3. Add another valid event after the bad ones.
    env["actions_file"].write_text("idx=[0/1] action=ARCHIVE status=STARTED\nidx=[0/2] action=RESTORE status=WAITING\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]))

    # 4. Run the stats tool once. caplog will capture its logs, and capsys will capture its stdout.
    with caplog.at_level(logging.WARNING):
        run_cli(stats_main, '-c', str(env["stats_config"]))

    # 5. Assert that the tool logged the expected warnings.
    # We check caplog.text, which is the string representation of all captured logs.
    assert "Could not parse event" in caplog.text
    assert "skipping" in caplog.text

    # 6. Assert that the final JSON output (captured by capsys) is correct,
    # proving the tool recovered and built the correct state.
    captured_stdout = capsys.readouterr().out
    stats_json = json.loads(captured_stdout)
    assert stats_json['summary']['total_live_actions'] == 2
