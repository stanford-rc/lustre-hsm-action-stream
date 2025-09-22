# Lustre HSM Action Stream

[![License](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

A lightweight toolkit for shipping Lustre HSM events from MDT `hsm/actions` logs to a Redis stream.

---

## Components

This project provides three command-line tools:

### `hsm-action-shipper`

A daemon designed for low-touch, reliable operation on each Lustre Metadata Server (MDS). It monitors `hsm/actions` files, processes log entries, and ships them as structured events to a central Redis stream.

#### Key features

*   **Purged event detection:** Detects when an action is removed from the `hsm/actions` file by comparing the live state against a local cache, then emits a `PURGED` event. This provides a complete `NEW -> UPDATE -> PURGED` event lifecycle.
*   **Persistent state cache:** Recovers its state from disk (`/var/cache/hsm-action-shipper/cache.json`) on restart. This prevents event storms and correctly identifies actions that were purged while the daemon was offline.
*   **Graceful shutdown:** Responds to `SIGINT` and `SIGTERM` by finishing the current poll cycle and saving the cache before exiting.
*   **Resilient Redis connection:** If the Redis server is unavailable, the shipper does not drop data. It attempts to reconnect with exponential backoff and retries sending any failed events on the next cycle.
*   **Low overhead:** Uses an efficient polling and event-batching pipeline to minimize performance impact on the MDS.

### `hsm-stream-janitor`

A daemon that prevents the Redis action stream from growing infinitely. It runs in the background, continuously tracking the state of all live actions. Periodically, it calculates the oldest event ID that is still part of an active operation and safely trims all earlier, unnecessary events from the stream.

### `hsm-stream-reconciler`

A command-line utility to validate the integrity of the Redis stream data against the "ground truth" of the Lustre/HSM filesystem.

#### How it works

The reconciler performs a three-step process to find discrepancies:

1.  **Builds ground truth state:** It scans all local `hsm/actions` files and builds an in-memory map of every live action and its status: `{(mdt, cat_idx, rec_idx): status}`.
2.  **Builds stream-derived state:** It reads the *entire* event stream from the beginning (`0-0`), replaying all `NEW`, `UPDATE`, and `PURGED` events to build a second in-memory map of what the state *should be*.
3.  **Compares and reports:** It compares the two maps and reports on:
    *   **Missing from stream:** Actions present in the filesystem but not in the stream's state.
    *   **Extra in stream:** Actions present in the stream's state but not in the filesystem.
    *   **Mismatched status:** Actions present in both but with different statuses (often a benign race condition).

### `hsm-action-top`

A `top`-like terminal dashboard for real-time monitoring of HSM activity across the filesystem. It connects to the Redis stream and provides a live, aggregated view of all ongoing actions.

#### How it works

The viewer first replays the entire event stream to build an accurate picture of the current state. It then listens for live events, updating its statistics and refreshing the screen at a configurable interval. The **diff column** shows the change in the count for each group since the last refresh, highlighting trends and bursts of activity.

#### Example view

```
--- Lustre HSM Action Dashboard ---
Time: 2025-09-22 10:39:32 | Redis: Connected
Viewer Status: Bootstrap complete (269,124 events). Listening for live updates...
Live Actions: 268,506 | Last Event: 1s ago

--- Live Action Count by (MDT, Action, Status) ---
MDT         | ACTION  | STATUS     |      COUNT DIFF      
----------------------------------------------------------
elm-MDT0000 | ARCHIVE | STARTED    |     13,365 
elm-MDT0000 | ARCHIVE | SUCCEED    |        140 
elm-MDT0001 | ARCHIVE | STARTED    |     50,000 
elm-MDT0001 | ARCHIVE | WAITING    |     38,732 
elm-MDT0002 | ARCHIVE | STARTED    |    111,680 
elm-MDT0002 | ARCHIVE | SUCCEED    |          2 
elm-MDT0003 | ARCHIVE | STARTED    |     54,583 (+3,033)
elm-MDT0003 | ARCHIVE | WAITING    |          4 (+1)
```

---

## Redis stream event structure

Events are added to a Redis stream via `XADD`. Each entry contains a single field, `data`, holding a JSON-encoded string.

**Example JSON payload:**
```json
{
  "event_type": "UPDATE",
  "action": "ARCHIVE",
  "fid": "0x2000057b4:0x1d648:0x0",
  "status": "STARTED",
  "mdt": "elm-MDT0000",
  "cat_idx": 520,
  "rec_idx": 36052,
  "timestamp": 1758517756,
  "raw": "lrh=[type=10680000 len=192 idx=520/36052] fid=[0x2000057b4:0x1d648:0x0] dfid=[0x2000057b4:0x1d648:0x0] compound/cookie=0x0/0x6913bb97 action=ARCHIVE archive#=1 flags=0x0 extent=0x0-0xffffffffffffffff gid=0x0 datalen=50 status=STARTED data=[7461673D6D]"
}
```

---

## Installation

This project is packaged as an RPM. After installation, the following files and directories are created:

*   **System Binaries:** `/usr/sbin/hsm-action-shipper`, `/usr/sbin/hsm-stream-reconciler`
*   **User Binary:** `/usr/bin/hsm-action-top`
*   **Configuration:** `/etc/lustre-hsm-action-stream/hsm_action_shipper.yaml`
*   **Systemd Service:** `/usr/lib/systemd/system/hsm-action-shipper.service`
*   **Cache Directory:** `/var/cache/hsm-action-shipper/`

---

## Usage

### hsm-action-shipper

1.  **Configure:** Edit `/etc/lustre-hsm-action-stream/hsm_action_shipper.yaml` to set your `redis_host` and other parameters.

2.  **Enable and start the service:**
    ```bash
    systemctl enable --now hsm-action-shipper.service
    ```

3.  **Check status and logs:**
    ```bash
    systemctl status hsm-action-shipper.service
    journalctl -u hsm-action-shipper.service -f
    ```

### hsm-stream-janitor

The janitor should be run on a central utility server or on the Redis server itself. It doesn't need access to the Lustre filesystem.

1.  **Configure:** Edit `/etc/lustre-hsm-action-stream/hsm_stream_janitor.yaml` to point to your `redis_host`. The other defaults are generally safe.

2.  **Enable and start the service:**
    ```bash
    systemctl enable --now hsm-stream-janitor.service
    ```
3.  **Check logs:**
    ```bash
    journalctl -u hsm-stream-janitor.service -f
    ```

### hsm-stream-reconciler

Run on an MDS to validate the stream against that node's `hsm/actions` files.

```bash
# Validate against a specific Redis host, scoping to certain MDTs
hsm-stream-reconciler --host my-redis.internal --mdts elm-MDT0000 elm-MDT0001
```

### hsm-action-top

Run from any machine that can connect to Redis.

```bash
# Connect to a specific Redis host with a 2-second refresh interval
hsm-action-top --host my-redis.internal --interval 2
```
```
