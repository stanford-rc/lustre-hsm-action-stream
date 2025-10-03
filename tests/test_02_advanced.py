# tests/test_02_advanced.py

import pytest
import os
import sys
import json
import logging
from unittest.mock import patch
import redis

from lustre_hsm_action_stream.parser import parse_action_line
from lustre_hsm_action_stream.shipper import main as shipper_main
from lustre_hsm_action_stream.stats import main as stats_main

@pytest.mark.parametrize("line, expected", [
    (
        "lrh=[type=10680000 len=192 idx=517/31144] fid=[0x2800059ca:0xc464:0x0] action=ARCHIVE status=WAITING data=[...]",
        # Remove brackets from expected fid
        {'cat_idx': 517, 'rec_idx': 31144, 'fid': '0x2800059ca:0xc464:0x0', 'action': 'ARCHIVE', 'status': 'WAITING'}
    ),
    (
        "idx=[1/2] action=RESTORE fid=[0xabc] status=STARTED",
        {'cat_idx': 1, 'rec_idx': 2, 'fid': '0xabc', 'action': 'RESTORE', 'status': 'STARTED'}
    ),
    ("idx=[1] action=ARCHIVE fid=[0x1] status=STARTED", None), # This correctly has no fid in the expected output
    ("", None),
    ("some other kernel message", None)
])
def test_parse_action_line(line, expected):
    result = parse_action_line(line)
    if expected is None:
        assert result is None
    else:
        assert result is not None
        for key, value in expected.items():
            assert key in result
            assert result[key] == value

@pytest.fixture
def multi_mdt_test_env(tmp_path, redis_conn_params):
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
trim_chunk_size: 1000
use_approximate_trimming: false
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
    env["mdt0_actions_file"].write_text("idx=[0/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    env["mdt1_actions_file"].write_text("idx=[1/1] action=RESTORE fid=[0xb] status=WAITING\nidx=[1/2] action=RESTORE fid=[0xc] status=STARTED\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]), '--run-once')
    assert r.xlen(env["stream_name_0"]) == 1
    assert r.xlen(env["stream_name_1"]) == 2
    run_cli(stats_main, '-c', str(env["stats_config"]))
    stats_json = json.loads(capsys.readouterr().out)
    assert stats_json['summary']['total_live_actions'] == 3

def test_consumer_robustness_on_bad_data(test_env, redis_conn_params, caplog, capsys, run_cli):
    env = test_env
    r = redis.Redis(**redis_conn_params)
    env["actions_file"].write_text("idx=[0/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]), '--run-once')
    assert r.xlen(env["stream_name"]) == 1
    r.xadd(env["stream_name"], {"data": b"this is not json"})
    r.xadd(env["stream_name"], {"data": json.dumps({"mdt": "testfs-MDT0000", "cat_idx": 1, "rec_idx": 1})})
    env["actions_file"].write_text("idx=[0/1] action=ARCHIVE fid=[0xa] status=STARTED\nidx=[0/2] action=RESTORE fid=[0xb] status=WAITING\n")
    run_cli(shipper_main, '-c', str(env["shipper_config"]), '--run-once')
    assert r.xlen(env["stream_name"]) == 4
    with caplog.at_level(logging.WARNING):
        run_cli(stats_main, '-c', str(env["stats_config"]))
    assert "Could not parse event" in caplog.text
    captured_stdout = capsys.readouterr().out
    stats_json = json.loads(captured_stdout)
    assert stats_json['summary']['total_live_actions'] == 2
