# Common Operating Picture

A lightweight CoT (Cursor on Target) microservice that ingests position and intelligence data, stores entities in memory, and broadcasts changes to connected clients in real time. Includes a bidirectional bridge to [peat-chat](../peat-chat) and a live terminal dashboard for monitoring activity.

## Architecture

```
                                ┌──────────────┐
  CoT XML ──POST /cot──────────▶              │
                                │  COP Server  │──── WS :8080 ────▶ clients
  GET /cot ◀────────────────────│  (Express)   │
                                │  :3000       │
                                └──────┬───────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              │                        │                        │
     ┌────────▼─────────┐    ┌────────▼─────────┐    ┌────────▼─────────┐
     │   cotStore.js     │    │   cotBridge.js    │    │  cop-monitor.py  │
     │   In-memory Map   │    │   peat-chat ↔ COP │    │  Live TUI        │
     │   + delta engine  │    │   bidirectional   │    │  dashboard       │
     └──────────────────┘    └──────────────────┘    └──────────────────┘
```

## Quick Start

```bash
# Install dependencies
npm install

# Start the COP server
node index.js
```

The server starts two listeners:
- **HTTP API** on port `3000` — receives and serves CoT data
- **WebSocket** on port `8080` — broadcasts entity deltas to connected clients

---

## COP Server

### POST /cot — Submit CoT Data

Send a standard CoT XML event to ingest an entity:

```bash
curl -X POST http://localhost:3000/cot \
  -H "Content-Type: application/xml" \
  -d '<event uid="ALPHA-1" type="a-f-G-U-C" how="h-e"
        time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
        stale="2026-04-07T13:00:00Z">
    <point lat="38.8977" lon="-77.0365" hae="0" ce="999999" le="999999"/>
  </event>'
```

**Response** (JSON):
```json
{
  "status": "ok",
  "delta": {
    "event": {
      "$": { "uid": "ALPHA-1", "type": "a-f-G-U-C", ... },
      "point": [{ "$": { "lat": "38.8977", "lon": "-77.0365", ... } }]
    }
  }
}
```

When the same UID is submitted again with changed fields, the response contains only the delta:

```json
{
  "status": "ok",
  "delta": { "uid": "ALPHA-1", "changes": { "lat": "38.9000" } }
}
```

### GET /cot — Retrieve All Entities

```bash
curl http://localhost:3000/cot
```

Returns a JSON array of all stored entities.

### WebSocket — Real-time Deltas

Connect to `ws://localhost:8080` to receive live entity updates. On connection you receive a welcome message, then every subsequent message is a JSON delta (same format as the POST response `delta` field).

```javascript
const ws = new WebSocket('ws://localhost:8080');
ws.onmessage = (e) => console.log(JSON.parse(e.data));
```

---

## CoT Bridge (`cotBridge.js`)

A standalone relay that connects the COP to a running [peat-chat](../peat-chat) server, allowing CoT entities to flow bidirectionally between both systems.

### How It Works

```
peat-chat :8090/ws              cotBridge.js              COP :3000 + :8080
      │                              │                          │
      │── cot_state (every 5s) ─────▶│                          │
      │                              │── POST /cot (XML) ──────▶│
      │                              │                          │
      │                              │◀── WS delta (JSON) ──────│
      │◀── cot_position / marker ────│                          │
```

- **peat-chat → COP**: The bridge listens for `cot_state` broadcasts from peat-chat (emitted every 5 seconds). Each contact and marker is converted to CoT XML and POSTed to the COP's `/cot` endpoint.
- **COP → peat-chat**: The bridge listens on the COP WebSocket for entity deltas. New position entities are forwarded as `cot_position` messages, and new marker entities are forwarded as `create_marker` messages to peat-chat.
- **Echo prevention**: Two origin-tracking sets ensure data never ping-pongs. A UID that originated from peat-chat is never forwarded back to peat-chat when COP re-broadcasts it (and vice versa).

### Prerequisites

Both services must be running before the bridge starts:

```bash
# Terminal 1 — COP server
node index.js

# Terminal 2 — peat-chat server (from the peat-chat repo)
cd ../peat-chat/server && go run .
```

### Running the Bridge

```bash
# Default settings (connects to localhost, joins room "cop-bridge")
node cotBridge.js
```

### Configuration

All settings are environment variables with sensible defaults:

| Variable | Default | Description |
|---|---|---|
| `PEAT_WS_URL` | `ws://localhost:8090/ws` | peat-chat WebSocket endpoint |
| `PEAT_ROOM_NAME` | `cop-bridge` | Room the bridge joins in peat-chat |
| `COP_HTTP_URL` | `http://localhost:3000` | COP REST API base URL |
| `COP_WS_URL` | `ws://localhost:8080` | COP WebSocket endpoint |
| `BRIDGE_NAME` | `COP-Bridge` | Callsign shown in peat-chat |
| `RECONNECT_MS` | `3000` | Reconnect delay in milliseconds |

**Examples:**

```bash
# Join an existing peat-chat room called "operations"
PEAT_ROOM_NAME=operations node cotBridge.js

# Connect to remote hosts
PEAT_WS_URL=ws://10.0.0.5:8090/ws COP_HTTP_URL=http://10.0.0.10:3000 node cotBridge.js

# Custom bridge name
BRIDGE_NAME=TAK-Relay PEAT_ROOM_NAME=general node cotBridge.js
```

### Auto-Reconnect

If either WebSocket connection drops, the bridge automatically reconnects after the configured delay (default 3 seconds). On reconnect to COP, the bridge re-seeds its entity cache via `GET /cot`. On reconnect to peat-chat, it re-runs the handshake (identity → set_name → join_room).

### Data Translation Reference

| peat-chat | Direction | COP |
|---|---|---|
| `CotContact` (JSON) | → | CoT XML `<event type="a-...">` |
| `CotMarker` (JSON) | → | CoT XML `<event type="b-...">` |
| CoT XML new entity | ← | `cot_position` message |
| CoT XML marker entity | ← | `create_marker` message |
| Delta update (partial) | ← | `cot_position` (merged with cache) |

---

## COP Monitor (`cop-monitor.py`)

A live terminal dashboard built with [Rich](https://github.com/Textualize/rich) that displays CoT activity in real time. Connects to the COP WebSocket and HTTP API to show entities, connection status, and an activity feed.

### Install Dependencies

```bash
pip install rich websocket-client
```

> If your system Python is PEP 668 protected, use a virtual environment:
> ```bash
> python3 -m venv .venv && source .venv/bin/activate && pip install rich websocket-client
> ```

### Running the Monitor

```bash
python3 cop-monitor.py
```

### Dashboard Layout

```
┌══════════════════════════════════════════════════════════════════╗
║  ◆ COMMON OPERATING PICTURE                                     ║
║  COP: ws://localhost:8080  │  Entities: 5  │  Deltas: 42       ║
╠═══════════════════════════════╤══════════════════════════════════╣
│ CoT Entities (5)              │ Status                          │
│ UID      TYPE       LAT   LON │ ✓ WebSocket                    │
│ ALPHA-1  a-f-G-U-C  38.9 -77 │ ✓ HTTP API                     │
│ WP-3     b-m-p-w    39.1 -76 │                                 │
│                               │ Deltas/min  8.4                │
│                               │ a-f  ████ 4                    │
│                               │ b-m  █ 1                       │
├───────────────────────────────┴─────────────────────────────────┤
│ Activity Feed (42)                                              │
│ 09:15:32  ✦ NEW   a-f-G-U-C  uid=ALPHA-1  38.8977°N 77.0365°W │
│ 09:15:35  ◀ RECV  a-f-G-U-C  uid=ALPHA-1  38.9000°N 77.0300°W │
│ 09:15:35  ▶ CAST  uid=ALPHA-1  changed: lat, lon               │
└─────────────────────────────────────────────────────────────────┘
```

**Panels:**

- **Header** — COP endpoint, total entity count, delta count, uptime
- **CoT Entities** — Table of all tracked entities (UID, CoT type, lat/lon, last seen timestamp). Sorted by most recently updated. Color-coded by affiliation: green (friendly `a-f`), red (hostile `a-h`), yellow (neutral `a-n`), cyan (markers `b-m`)
- **Status** — Live connection indicators for WebSocket and HTTP API, delta rate, and a bar chart breakdown by CoT type prefix
- **Activity Feed** — Rolling log of the last 20 events with direction indicators: `✦ NEW` (first appearance), `◀ RECV` (position update), `▶ CAST` (delta with changed fields)

### Options

```bash
python3 cop-monitor.py --ws ws://10.0.0.5:8080 --http http://10.0.0.5:3000
```

| Flag | Default | Description |
|---|---|---|
| `--ws` | `ws://localhost:8080` | COP WebSocket URL |
| `--http` | `http://localhost:3000` | COP HTTP API URL |

The monitor auto-reconnects if the COP WebSocket drops and polls the HTTP API every 5 seconds to sync entity state.

---

## Running Everything Together

Open three terminals:

```bash
# Terminal 1 — COP server
node index.js

# Terminal 2 — CoT bridge (optional, only if using peat-chat)
PEAT_ROOM_NAME=general node cotBridge.js

# Terminal 3 — Live monitor
python3 cop-monitor.py
```

Then send test data to see it flow through:

```bash
# Send a friendly ground unit
curl -s -X POST http://localhost:3000/cot \
  -H "Content-Type: application/xml" \
  -d '<event uid="BRAVO-2" type="a-f-G-U-C" how="h-e"
        time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
        stale="2026-04-07T13:00:00Z">
    <point lat="39.0997" lon="-94.5786" hae="0" ce="999999" le="999999"/>
  </event>'

# Update its position
curl -s -X POST http://localhost:3000/cot \
  -H "Content-Type: application/xml" \
  -d '<event uid="BRAVO-2" type="a-f-G-U-C" how="h-e"
        time="2026-04-07T12:01:00Z" start="2026-04-07T12:01:00Z"
        stale="2026-04-07T13:01:00Z">
    <point lat="39.1050" lon="-94.5800" hae="0" ce="999999" le="999999"/>
  </event>'

# Add a waypoint marker
curl -s -X POST http://localhost:3000/cot \
  -H "Content-Type: application/xml" \
  -d '<event uid="WP-RALLY" type="b-m-p-w" how="h-e"
        time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
        stale="2026-04-08T12:00:00Z">
    <point lat="38.8977" lon="-77.0365" hae="0" ce="999999" le="999999"/>
  </event>'
```

All three events will appear in the monitor's activity feed in real time. If the bridge is running, they will also appear on the peat-chat map.
