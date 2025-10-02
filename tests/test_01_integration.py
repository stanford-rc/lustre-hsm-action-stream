# tests/test_01_integration.py

import pytest
import os
import sys
import json
from unittest.mock import patch, MagicMock

# Imports from the application
from lustre_hsm_action_stream.shipper import main as shipper_main
from lustre_hsm_action_stream.stats import main as stats_main
from lustre_hsm_action_stream.viewer import main as viewer_main, HSMStateTracker
from lustre_hsm_action_stream.reconciler import main as reconciler_main
from lustre_hsm_action_stream.tail import main as tail_main
from lustre_hsm_action_stream.consumer import StreamEvent, StreamReader
import redis

def test_shipper_new_update_purge(test_env, redis_conn_params, run_cli):
    actions_file, config_file, stream_name = test_env["actions_file"], test_env["shipper_config"], test_env["stream_name"]
    r = redis.Redis(**redis_conn_params, decode_responses=True)
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\nidx=[1/2] action=RESTORE fid=[0xb] status=WAITING\n")
    run_cli(shipper_main, '-c', str(config_file))
    assert r.xlen(stream_name) == 2
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=SUCCEED\nidx=[1/2] action=RESTORE fid=[0xb] status=WAITING\n")
    run_cli(shipper_main, '-c', str(config_file))
    assert r.xlen(stream_name) == 3
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=SUCCEED\n")
    run_cli(shipper_main, '-c', str(config_file))
    assert r.xlen(stream_name) == 4
    actions_file.write_text("")
    run_cli(shipper_main, '-c', str(config_file))
    assert r.xlen(stream_name) == 5

def test_shipper_reconciler_self_healing(test_env, redis_conn_params, run_cli):
    actions_file, config_file, stream_name = test_env["actions_file"], test_env["shipper_config"], test_env["stream_name"]
    r = redis.Redis(**redis_conn_params, decode_responses=True)
    orphan_event = {"event_type": "NEW", "mdt": test_env["mdt_name"], "cat_idx": 99, "rec_idx": 99}
    r.xadd(stream_name, {"data": json.dumps(orphan_event)})
    assert r.xlen(stream_name) == 1
    actions_file.write_text("")
    run_cli(shipper_main, '-c', str(config_file), '--maintenance-now')
    assert r.xlen(stream_name) == 2
    last_event = r.xrevrange(stream_name, count=1)[0][1]
    last_event_data = json.loads(last_event['data'])
    assert last_event_data['event_type'] == 'PURGED' and last_event_data['cat_idx'] == 99 and last_event_data['action'] == 'RECONCILED'

def test_stats_tool(test_env, capsys, run_cli):
    actions_file, shipper_config, stats_config = test_env["actions_file"], test_env["shipper_config"], test_env["stats_config"]
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\nidx=[1/2] action=RESTORE fid=[0xb] status=WAITING\n")
    run_cli(shipper_main, '-c', str(shipper_config))
    run_cli(stats_main, '-c', str(stats_config))
    stats_json = json.loads(capsys.readouterr().out)
    assert stats_json['summary']['total_live_actions'] == 2 and stats_json['summary']['total_events_replayed'] == 2
    actions_file.write_text("idx=[1/2] action=RESTORE fid=[0xb] status=WAITING\n")
    run_cli(shipper_main, '-c', str(shipper_config))
    run_cli(stats_main, '-c', str(stats_config))
    stats_json2 = json.loads(capsys.readouterr().out)
    assert stats_json2['summary']['total_live_actions'] == 1 and stats_json2['summary']['total_events_replayed'] == 3

def test_viewer_state_machine(test_env, redis_conn_params, run_cli):
    actions_file, shipper_config = test_env["actions_file"], test_env["shipper_config"]
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    run_cli(shipper_main, '-c', str(shipper_config))
    reader = StreamReader(prefix='hsm:actions', **redis_conn_params)
    tracker = HSMStateTracker(reader)
    for event in reader.events(from_beginning=True, block_ms=200):
        if event:
            with tracker.lock: tracker._process_one_event(event)
        else: break
    summary = tracker.get_summary()
    assert summary['live_action_count'] == 1 and summary['total_events'] == 1
    table_item = summary['summary_table'][0]
    assert table_item['key'] == (test_env['mdt_name'], 'ARCHIVE', 'STARTED') and table_item['count'] == 1

def test_viewer_cli_smoke_test(test_env, capsys, redis_conn_params, run_cli):
    actions_file, shipper_config = test_env["actions_file"], test_env["shipper_config"]
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    run_cli(shipper_main, '-c', str(shipper_config))
    try:
        with patch('subprocess.run', MagicMock(return_value=MagicMock(returncode=1))):
            run_cli(
                viewer_main,
                '--stream-prefix', 'hsm:actions',
                '--run-once',
                '--host', redis_conn_params['host'],
                '--port', str(redis_conn_params['port']),
                '--db', str(redis_conn_params['db'])
            )
    except Exception as e:
        pytest.fail(f"viewer --run-once crashed with an exception: {e}")
    captured = capsys.readouterr()
    assert "Lustre HSM Action Dashboard" in captured.out
    assert "Live Actions: 1" in captured.out

def test_reconciler_tool(test_env, capsys, redis_conn_params, run_cli):
    """
    Tests the hsm-stream-reconciler tool in both success and failure scenarios.
    """
    actions_file = test_env["actions_file"]
    shipper_config = test_env["shipper_config"]

    # --- 1. Test the SUCCESS case (Filesystem and Redis are in sync) ---
    print("\n--- Testing Reconciler (Success Case) ---")

    # Setup: Create one live action and ship it.
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    run_cli(shipper_main, '-c', str(shipper_config))

    # Run the reconciler. It should not raise an exception and exit with code 0.
    run_cli(
        reconciler_main,
        '--glob', str(actions_file),
        '--stream-prefix', 'hsm:actions',
        '--host', redis_conn_params['host'],
        '--port', str(redis_conn_params['port']),
        '--db', str(redis_conn_params['db'])
    )

    captured = capsys.readouterr()
    assert "SUCCESS: Validation complete" in captured.out
    assert "FAILURE" not in captured.out

    # --- 2. Test the FAILURE case (Orphan action in Redis) ---
    print("\n--- Testing Reconciler (Failure Case) ---")

    # Setup: Create an inconsistency by clearing the actions file.
    actions_file.write_text("")

    # Run the reconciler. We expect it to fail and exit with code 1.
    with pytest.raises(SystemExit) as e:
        run_cli(
            reconciler_main,
            '--glob', str(actions_file),
            '--stream-prefix', 'hsm:actions',
            '--host', redis_conn_params['host'],
            '--port', str(redis_conn_params['port']),
            '--db', str(redis_conn_params['db'])
        )

    assert e.value.code == 1, "Reconciler should exit with code 1 on failure"

    captured_fail = capsys.readouterr()
    assert "FAILURE: Critical discrepancies found" in captured_fail.out
    assert "ERROR: 1 actions found in stream state but are PURGED" in captured_fail.out

def test_stats_tool_output_structure(test_env, capsys, run_cli):
    """
    Validates that the hsm-stream-stats output conforms to the expected JSON structure
    with top-level 'summary', 'streams', and 'breakdown' keys.
    """
    actions_file = test_env["actions_file"]
    shipper_config = test_env["shipper_config"]
    stats_config = test_env["stats_config"]

    # 1. Populate the stream with at least one action to ensure output is not empty.
    actions_file.write_text("idx=[1/1] action=ARCHIVE fid=[0xa] status=STARTED\n")
    run_cli(shipper_main, '-c', str(shipper_config))

    # 2. Run the stats tool and capture its JSON output.
    run_cli(stats_main, '-c', str(stats_config))
    captured = capsys.readouterr()
    stats_json = json.loads(captured.out)

    # 3. Assert the top-level structure is correct.
    assert set(stats_json.keys()) == {'summary', 'streams', 'breakdown'}

    # 4. Validate the structure of the 'summary' object.
    assert isinstance(stats_json['summary'], dict)
    assert 'total_live_actions' in stats_json['summary']

    # 5. Validate the structure of the 'streams' list.
    assert isinstance(stats_json['streams'], list)
    assert len(stats_json['streams']) > 0
    first_stream_obj = stats_json['streams'][0]
    assert isinstance(first_stream_obj, dict)
    assert {'mdt', 'length', 'age_seconds', 'newest_entry_age_seconds'}.issubset(first_stream_obj.keys())

    # 6. Validate the structure of the 'breakdown' list.
    assert isinstance(stats_json['breakdown'], list)
    assert len(stats_json['breakdown']) > 0
    first_breakdown_obj = stats_json['breakdown'][0]
    assert isinstance(first_breakdown_obj, dict)
    assert set(first_breakdown_obj.keys()) == {'mdt', 'action', 'status', 'count'}

def test_tail_tool_output_and_filtering(capsys):
    """
    Tests the hsm-stream-tail tool's output formatting and filtering logic
    by mocking the event stream and the 'lfs' command.
    """
    # 1. Define mock events that our fake stream will yield.
    started_event = StreamEvent(
        stream='hsm:actions:test-MDT0000',
        id='1-0',
        data={
            "event_type": "UPDATE", "action": "ARCHIVE", "status": "STARTED",
            "mdt": "test-MDT0000", "fid": "[0xa:0xb:0xc]", "timestamp": 1700000000
        }
    )
    purged_event = StreamEvent(
        stream='hsm:actions:test-MDT0000',
        id='1-1',
        data={
            "event_type": "PURGED", "mdt": "test-MDT0000", "cat_idx": 1, "rec_idx": 1,
            "timestamp": 1700000001
        }
    )

    # 2. Use patch to mock both the event source and the external 'lfs' command.
    with patch('lustre_hsm_action_stream.consumer.StreamReader.events') as mock_events, \
         patch('subprocess.run') as mock_subprocess:

        # --- Test A: Basic formatting and FID resolution ---
        print("\n--- Testing Tail (Basic Formatting) ---")
        mock_events.return_value = [started_event]
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout="/lustre/mock/path/to/file.dat\n"
        )

        # We call main() directly after mocking sys.argv to avoid the run_cli helper
        with patch.object(sys, 'argv', ['hsm-stream-tail', '--mountpoint', '/lustre']):
            try:
                tail_main()
            except SystemExit: # The main function calls sys.exit(0) at the end
                pass

        captured = capsys.readouterr()
        # Assert that the output contains all the expected formatted parts
        assert "ARCHIVE" in captured.out
        assert "STARTED" in captured.out
        assert "/lustre/mock/path/to/file.dat" in captured.out
        assert "(id: 1-0)" in captured.out

        # --- Test B: Default filtering (should hide PURGED) ---
        print("\n--- Testing Tail (Default Filtering) ---")
        mock_events.return_value = [started_event, purged_event]
        with patch.object(sys, 'argv', ['hsm-stream-tail', '--mountpoint', '/lustre']):
            try: tail_main()
            except SystemExit: pass

        captured = capsys.readouterr()
        assert "ARCHIVE" in captured.out  # The STARTED event should be visible
        assert "PURGED" not in captured.out # The PURGED event should be hidden by default

        # --- Test C: Using --show to override default filter ---
        print("\n--- Testing Tail (--show PURGED) ---")
        mock_events.return_value = [started_event, purged_event]
        with patch.object(sys, 'argv', ['hsm-stream-tail', '--mountpoint', '/lustre', '--show', 'PURGED']):
            try: tail_main()
            except SystemExit: pass

        captured = capsys.readouterr()
        assert "ARCHIVE" in captured.out # STARTED is still visible
        assert "PURGED" in captured.out  # PURGED is now also visible
