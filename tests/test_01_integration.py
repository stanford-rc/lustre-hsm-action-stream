# tests/test_01_integration.py

import pytest
import os
import sys
import json
from unittest.mock import patch, MagicMock

from lustre_hsm_action_stream.shipper import main as shipper_main
from lustre_hsm_action_stream.stats import main as stats_main
from lustre_hsm_action_stream.viewer import main as viewer_main, HSMStateTracker
from lustre_hsm_action_stream.reconciler import main as reconciler_main
from lustre_hsm_action_stream.tail import main as tail_main
from lustre_hsm_action_stream.consumer import StreamEvent, StreamReader
import redis

VALID_LINE_1 = "idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\n"
VALID_LINE_2 = "idx=[1/2] action=RESTORE fid=[0xb] status=WAITING\n"
VALID_LINE_1_SUCCEED = "idx=[1/1] action=ARCHIVE fid=[0xa] status=SUCCEED\n"

def test_shipper_new_update_purge(test_env, redis_conn_params, run_cli):
    actions_file, config_file, stream_name = test_env["actions_file"], test_env["shipper_config"], test_env["stream_name"]
    r = redis.Redis(**redis_conn_params, decode_responses=True)
    actions_file.write_text(VALID_LINE_1 + VALID_LINE_2)
    run_cli(shipper_main, '-c', str(config_file), '--run-once')
    assert r.xlen(stream_name) == 2
    actions_file.write_text(VALID_LINE_1_SUCCEED + VALID_LINE_2)
    run_cli(shipper_main, '-c', str(config_file), '--run-once')
    assert r.xlen(stream_name) == 3
    actions_file.write_text(VALID_LINE_1_SUCCEED)
    run_cli(shipper_main, '-c', str(config_file), '--run-once')
    assert r.xlen(stream_name) == 4
    actions_file.write_text("")
    run_cli(shipper_main, '-c', str(config_file), '--run-once')
    assert r.xlen(stream_name) == 5

def test_shipper_reconciler_self_healing(test_env, redis_conn_params, run_cli):
    actions_file, config_file, stream_name = test_env["actions_file"], test_env["shipper_config"], test_env["stream_name"]
    r = redis.Redis(**redis_conn_params, decode_responses=True)
    orphan_event = {
        "event_type": "NEW", "mdt": test_env["mdt_name"], "cat_idx": 99, "rec_idx": 99,
        "action": "ORPHANED", "status": "STARTED", "fid": "[0xdeadbeef]",
        "action_key": "[0xdeadbeef]:ORPHANED"
    }
    r.xadd(stream_name, {"data": json.dumps(orphan_event)})
    assert r.xlen(stream_name) == 1
    actions_file.write_text("")
    run_cli(shipper_main, '-c', str(config_file), '--maintenance-now')
    # The reconciler now injects a valid PURGED event (with a constructed action_key)
    # The replayer finds this purge, resulting in 0 live actions, and the trim succeeds.
    assert r.xlen(stream_name) == 0

def test_stats_tool(test_env, capsys, run_cli):
    actions_file, shipper_config, stats_config = test_env["actions_file"], test_env["shipper_config"], test_env["stats_config"]
    actions_file.write_text(VALID_LINE_1 + VALID_LINE_2)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    run_cli(stats_main, '-c', str(stats_config))
    stats_json = json.loads(capsys.readouterr().out)
    assert stats_json['summary']['total_live_actions'] == 2
    actions_file.write_text(VALID_LINE_2)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    run_cli(stats_main, '-c', str(stats_config))
    stats_json2 = json.loads(capsys.readouterr().out)
    assert stats_json2['summary']['total_live_actions'] == 1

def test_viewer_state_machine(test_env, redis_conn_params, run_cli):
    actions_file, shipper_config = test_env["actions_file"], test_env["shipper_config"]
    actions_file.write_text(VALID_LINE_1)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    reader = StreamReader(prefix='hsm:actions', **redis_conn_params)
    tracker = HSMStateTracker(reader)
    # Use run-once behavior.
    tracker.run_once()
    summary = tracker.get_summary()
    assert summary['live_action_count'] == 1
    table_item = summary['summary_table'][0]
    assert table_item['key'] == (test_env['mdt_name'], 'ARCHIVE', 'STARTED')

def test_viewer_cli_smoke_test(test_env, capsys, redis_conn_params, run_cli):
    actions_file, shipper_config = test_env["actions_file"], test_env["shipper_config"]
    actions_file.write_text(VALID_LINE_1)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    run_cli(
        viewer_main,
        '--stream-prefix', 'hsm:actions', '--run-once',
        '--host', redis_conn_params['host'], '--port', str(redis_conn_params['port']), '--db', str(redis_conn_params['db'])
    )
    captured = capsys.readouterr()
    assert "Live Actions: 1" in captured.out

def test_reconciler_tool(test_env, capsys, redis_conn_params, run_cli):
    actions_file, shipper_config = test_env["actions_file"], test_env["shipper_config"]
    actions_file.write_text(VALID_LINE_1)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    run_cli(
        reconciler_main,
        '--glob', str(actions_file), '--stream-prefix', 'hsm:actions',
        '--host', redis_conn_params['host'], '--port', str(redis_conn_params['port']), '--db', str(redis_conn_params['db'])
    )
    captured = capsys.readouterr()
    assert "SUCCESS: Validation complete" in captured.out
    actions_file.write_text("")
    with pytest.raises(SystemExit) as e:
        run_cli(
            reconciler_main,
            '--glob', str(actions_file), '--stream-prefix', 'hsm:actions',
            '--host', redis_conn_params['host'], '--port', str(redis_conn_params['port']), '--db', str(redis_conn_params['db'])
        )
    assert e.value.code == 1
    captured_fail = capsys.readouterr()
    assert "FAILURE: Critical discrepancies found" in captured_fail.out

def test_stats_tool_output_structure(test_env, capsys, run_cli):
    actions_file, shipper_config, stats_config = test_env["actions_file"], test_env["shipper_config"], test_env["stats_config"]
    actions_file.write_text(VALID_LINE_1)
    run_cli(shipper_main, '-c', str(shipper_config), '--run-once')
    run_cli(stats_main, '-c', str(stats_config))
    captured = capsys.readouterr()
    stats_json = json.loads(captured.out)
    assert set(stats_json.keys()) == {'summary', 'streams', 'breakdown'}

def test_tail_tool_output_and_filtering(test_env, capsys, run_cli):
    """
    Tests the hsm-stream-tail tool's output formatting and filtering logic
    by mocking the event stream and the 'lfs' command.
    """
    # FIX: Correctly instantiate the StreamEvent namedtuples
    started_event = StreamEvent(
        stream='hsm:actions:test-MDT0000',
        id='1-0',
        data={ "event_type": "UPDATE", "action": "ARCHIVE", "status": "STARTED", "mdt": "test-MDT0000", "fid": "[0xa:0xb:0xc]", "timestamp": 1700000000, "action_key": "[0xa:0xb:0xc]:ARCHIVE" }
    )
    purged_event = StreamEvent(
        stream='hsm:actions:test-MDT0000',
        id='1-1',
        data={ "event_type": "PURGED", "mdt": "test-MDT0000", "cat_idx": 1, "rec_idx": 1, "timestamp": 1700000001, "action_key": "unknown:1:1" }
    )

    with patch('lustre_hsm_action_stream.consumer.StreamReader.events') as mock_events, \
         patch('subprocess.run') as mock_subprocess, \
         patch('shutil.which', return_value='/usr/bin/lfs'):

        # --- Test A: Basic formatting and FID resolution ---
        mock_events.return_value = [started_event, None] # Add None to simulate end of stream for run_once
        mock_subprocess.return_value = MagicMock(returncode=0, stdout="/lustre/mock/path/to/file.dat\n")

        run_cli(tail_main, '--run-once', '--mountpoint', '/lustre')

        captured = capsys.readouterr()
        assert "ARCHIVE" in captured.out and "STARTED" in captured.out and "/lustre/mock/path/to/file.dat" in captured.out

        # --- Test B: Default filtering (should hide PURGED) ---
        mock_events.return_value = [started_event, purged_event, None]

        run_cli(tail_main, '--run-once', '--mountpoint', '/lustre')

        captured = capsys.readouterr()
        assert "ARCHIVE" in captured.out and "PURGED" not in captured.out

        # --- Test C: Using --show to override default filter ---
        mock_events.return_value = [started_event, purged_event, None]

        run_cli(tail_main, '--run-once', '--mountpoint', '/lustre', '--show', 'PURGED')

        captured = capsys.readouterr()
        assert "ARCHIVE" in captured.out and "PURGED" in captured.out
