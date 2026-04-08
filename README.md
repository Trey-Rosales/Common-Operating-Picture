# Common Operating Picture

A lightweight CoT (Cursor on Target) microservice that ingests position and intelligence data, stores entities in memory, and broadcasts changes to connected clients in real time. Includes a bidirectional bridge to [PeatLink](../peat-chat) with mDNS auto-discovery, multicast UDP ingestion (ATAK/WinTAK compatible), and a live terminal dashboard for monitoring activity.

## Architecture

```
  UDP 239.2.3.1:6969 ─┐          ┌──────────────┐
  (ATAK/WinTAK)       │          │              │
                       ├─────────▶  COP Server  │──── WS :8080 ────▶ clients
  CoT XML ──POST /cot─┘          │  (Express)   │
  GET /cot ◀──────────────────────│  :3000       │
                                  └──────┬───────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
     ┌────────▼─────────┐    ┌──────────▼──────────┐    ┌─────────▼────────┐
     │   cotStore.js     │    │   cotBridge.js       │    │  cop-monitor.py  │
     │   In-memory Map   │    │   PeatLink ↔ COP     │    │  Live TUI        │
     │   + delta engine  │    │   + mDNS discovery   │    │  dashboard       │
     └──────────────────┘    │   + UDP multicast    │    └──────────────────┘
                              └──────────────────────┘
```

## Project Structure

```
Common-Operating-Picture/
├── index.js              # Express HTTP server + WebSocket init (entry point)
├── cotStore.js           # In-memory entity store with delta computation
├── wsServer.js           # WebSocket broadcast server (port 8080)
├── cotBridge.js          # Bidirectional PeatLink ↔ COP relay + mDNS + UDP
├── cop-monitor.py        # Live terminal dashboard (Rich TUI)
├── cotStore.test.js      # Tests for entity store and delta logic
├── cotBridge.test.js     # Tests for XML translation, echo prevention, source tagging
├── package.json          # Node dependencies and scripts
├── .gitignore
└── README.md
```

## Quick Start

```bash
# 1. Install Node dependencies
npm install

# 2. Start the COP server
node index.js

# 3. (Optional) Start the bridge — auto-discovers PeatLink on the LAN
node cotBridge.js

# 4. (Optional) Start the live dashboard
pip install rich websocket-client
python3 cop-monitor.py
```

The server starts two listeners:
- **HTTP API** on port `3000` — receives and serves CoT data
- **WebSocket** on port `8080` — broadcasts entity deltas to connected clients

## Development

### Prerequisites

- **Node.js** >= 18 (tested on v20)
- **Python** >= 3.8 (only for the TUI monitor)
- **npm** (ships with Node.js)

### Install

```bash
npm install
```

### Run Tests

Tests use Node.js built-in test runner (`node:test`) — no extra test framework needed.

```bash
npm test
```

This runs all `*.test.js` files. Current test coverage:

| Suite | What it tests |
|---|---|
| `cotStore.test.js` | Entity storage, delta computation (new entities, updates, empty changes), `getAll()` |
| `cotBridge.test.js` | XML generation (`contactToXml`, `markerToXml`), icon mapping, echo prevention logic, UDP XML round-trip parsing, source tagging |

### Linting / Formatting

No linter is configured yet. The codebase uses CommonJS (`require`) — the `"type": "commonjs"` field is set in `package.json`.

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
      "$": { "uid": "ALPHA-1", "type": "a-f-G-U-C", "how": "h-e", "time": "...", "start": "...", "stale": "..." },
      "point": [{ "$": { "lat": "38.8977", "lon": "-77.0365", "hae": "0", "ce": "999999", "le": "999999" } }]
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

#### Source Tagging

Requests can include an `X-CoT-Source` header to identify where the data originated. The source value is attached to the WebSocket broadcast as a `_source` field so downstream consumers (like the TUI monitor) can display provenance.

```bash
# Tag data as coming from an ATAK device
curl -X POST http://localhost:3000/cot \
  -H "Content-Type: application/xml" \
  -H "X-CoT-Source: atak-device" \
  -d '<event uid="TAK-1" ...>...</event>'
```

The bridge automatically sets `X-CoT-Source` to `peatlink` or `udp-multicast` for data it forwards. Direct POSTs without the header default to `direct`.

### GET /cot — Retrieve All Entities

```bash
curl http://localhost:3000/cot
```

Returns a JSON array of all stored entities.

### WebSocket — Real-time Deltas

Connect to `ws://localhost:8080` to receive live entity updates. On connection you receive a welcome message, then every subsequent message is a JSON delta.

```javascript
const ws = new WebSocket('ws://localhost:8080');
ws.onmessage = (e) => {
  const delta = JSON.parse(e.data);
  // delta._source is "peatlink", "udp-multicast", or "direct"
  console.log(delta);
};
```

**New entity broadcast:**
```json
{
  "event": {
    "$": { "uid": "ALPHA-1", "type": "a-f-G-U-C", ... },
    "point": [{ "$": { "lat": "38.8977", "lon": "-77.0365", ... } }]
  },
  "_source": "peatlink"
}
```

**Update delta broadcast:**
```json
{
  "uid": "ALPHA-1",
  "changes": { "lat": "38.9000", "lon": "-77.0300" },
  "_source": "udp-multicast"
}
```

### Delta Engine (`cotStore.js`)

The store keeps an in-memory `Map<uid, cotJson>`. When an entity is updated:

1. If the UID is new, the full parsed object is returned as the delta.
2. If the UID already exists, a field-by-field comparison produces a `{ uid, changes }` object containing only the attributes and point fields that differ.

This means WebSocket clients receive minimal payloads — only what changed.

---

## CoT Bridge (`cotBridge.js`)

A standalone relay that connects the COP to a running [PeatLink](../peat-chat) server, allowing CoT entities to flow bidirectionally between both systems. Also ingests CoT data from standard multicast UDP (ATAK/WinTAK compatible).

### How It Works

```
                                                          ┌──────────────┐
  PeatLink :8090/ws ──── cot_state (5s) ────▶             │              │
                                             cotBridge.js │  COP :3000   │
  PeatLink :8090/ws ◀── cot_position ──────               │  + :8080     │
                                             │            └──────────────┘
  UDP 239.2.3.1:6969 ── CoT XML ───────────▶│── POST /cot ──────▶│
                                             │                     │
          mDNS _peatlink._tcp ──▶ auto-discover PeatLink          │
```

#### mDNS Auto-Discovery

The PeatLink Go server advertises itself via mDNS/Zeroconf as `_peatlink._tcp` on the local network. On startup, the bridge uses the [`bonjour-service`](https://www.npmjs.com/package/bonjour-service) library to browse for this service. When found, it extracts the IPv4 address and port and connects automatically.

- **No configuration needed** if PeatLink and the bridge are on the same subnet.
- If no PeatLink is found within 10 seconds (configurable via `MDNS_TIMEOUT_MS`), the bridge falls back to `ws://localhost:8090/ws`.
- A background watcher keeps running continuously. If PeatLink moves to a different IP or restarts, the bridge detects the change and reconnects.
- Set `PEAT_WS_URL` explicitly to skip mDNS entirely (useful for cross-subnet or tunneled deployments).

#### Multicast UDP Ingestion

The bridge listens on `239.2.3.1:6969` — the standard SA (Situational Awareness) multicast group used by ATAK, WinTAK, and other TAK ecosystem tools. Any CoT XML arriving on this address is:

1. Parsed using `xml2js`
2. Forwarded to the COP server via `POST /cot` (raw XML, tagged `X-CoT-Source: udp-multicast`)
3. Forwarded to PeatLink as a `cot_position` or `create_marker` message (depending on CoT type prefix)

This means ATAK devices broadcasting on the default multicast group will automatically appear in both the COP and PeatLink without any configuration.

Disable with `UDP_ENABLED=0` if you don't need multicast ingestion.

#### Data Flow Detail

- **PeatLink → COP**: The bridge listens for `cot_state` broadcasts from PeatLink (emitted every 5s). Each `CotContact` and `CotMarker` is converted to CoT XML and POSTed to the COP `/cot` endpoint.
- **COP → PeatLink**: The bridge listens on the COP WebSocket for entity deltas. New position entities (`a-*` types) forward as `cot_position` messages, markers (`b-*` types) as `create_marker`.
- **UDP → COP + PeatLink**: Raw CoT XML from multicast is forwarded to both systems simultaneously.

#### Echo Prevention

Three origin-tracking sets prevent data from ping-ponging:

| Set | Tracks UIDs from | Blocks forwarding to |
|---|---|---|
| `peatOrigins` | PeatLink `cot_state` | COP → PeatLink path (prevents re-sending data PeatLink already has) |
| `copOrigins` | COP WebSocket deltas | PeatLink → COP path (prevents re-sending data COP already has) |
| `udpOrigins` | Multicast UDP | PeatLink → COP path (prevents duplicate forwarding) |

A UID is added to its origin set the first time it's seen from that source. Subsequent appearances from other sources check all sets before forwarding.

### Prerequisites

The COP server must be running. PeatLink can be discovered automatically or specified manually:

```bash
# Terminal 1 — COP server
node index.js

# Terminal 2 — PeatLink server (from peat-chat repo, optional — bridge auto-discovers)
cd ../peat-chat/server && go run .
```

### Running the Bridge

```bash
# Auto-discover PeatLink on the network via mDNS
node cotBridge.js

# Or specify PeatLink URL explicitly (skips mDNS)
PEAT_WS_URL=ws://10.0.0.5:8090/ws node cotBridge.js
```

### Configuration

All settings are environment variables with sensible defaults:

| Variable | Default | Description |
|---|---|---|
| `PEAT_WS_URL` | *(empty — uses mDNS)* | PeatLink WebSocket endpoint. Leave empty for auto-discovery |
| `PEAT_ROOM_NAME` | `cop-bridge` | Room the bridge joins in PeatLink |
| `COP_HTTP_URL` | `http://localhost:3000` | COP REST API base URL |
| `COP_WS_URL` | `ws://localhost:8080` | COP WebSocket endpoint |
| `BRIDGE_NAME` | `COP-Bridge` | Callsign shown in PeatLink |
| `RECONNECT_MS` | `3000` | Reconnect delay in milliseconds |
| `MDNS_TIMEOUT_MS` | `10000` | mDNS discovery timeout before falling back to localhost |
| `UDP_MULTICAST_ADDR` | `239.2.3.1` | Multicast group for CoT UDP reception |
| `UDP_MULTICAST_PORT` | `6969` | Multicast port for CoT UDP reception |
| `UDP_ENABLED` | `1` | Set to `0` to disable multicast UDP listener |

**Examples:**

```bash
# Auto-discover PeatLink, join room "operations"
PEAT_ROOM_NAME=operations node cotBridge.js

# Connect to a specific PeatLink host
PEAT_WS_URL=ws://10.0.0.5:8090/ws node cotBridge.js

# Disable UDP multicast (only bridge PeatLink ↔ COP)
UDP_ENABLED=0 node cotBridge.js

# Custom bridge name
BRIDGE_NAME=TAK-Relay PEAT_ROOM_NAME=general node cotBridge.js

# Custom multicast group (non-standard network)
UDP_MULTICAST_ADDR=239.10.10.1 UDP_MULTICAST_PORT=17012 node cotBridge.js
```

### PeatLink Handshake

When the bridge connects to PeatLink's WebSocket, it performs this sequence:

1. PeatLink sends `identity` → bridge stores its assigned ID (so it can filter out its own position from `cot_state`)
2. Bridge sends `set_name` with `BRIDGE_NAME` (e.g. "COP-Bridge")
3. Bridge sends `join_room` with `PEAT_ROOM_NAME`
4. PeatLink responds `room_joined` with the hex room ID → bridge uses this for all subsequent messages

### Auto-Reconnect

If either WebSocket connection drops, the bridge automatically reconnects after the configured delay (default 3 seconds). On reconnect to COP, the bridge re-seeds its entity cache via `GET /cot`. On reconnect to PeatLink, it re-runs the handshake (identity → set_name → join_room). The mDNS watcher runs continuously — if PeatLink moves to a different host, the bridge detects and reconnects.

### Data Translation Reference

| Source | Direction | Destination | Format |
|---|---|---|---|
| PeatLink `CotContact` | → | COP | CoT XML `<event type="a-...">` via POST /cot |
| PeatLink `CotMarker` | → | COP | CoT XML `<event type="b-...">` via POST /cot |
| COP new entity | → | PeatLink | `cot_position` JSON message |
| COP new marker | → | PeatLink | `create_marker` JSON message |
| COP delta update | → | PeatLink | `cot_position` (position merged from cache) |
| UDP multicast CoT XML | → | COP | POST /cot (raw XML passthrough) |
| UDP multicast CoT XML | → | PeatLink | `cot_position` or `create_marker` JSON |

---

## COP Monitor (`cop-monitor.py`)

A live terminal dashboard built with [Rich](https://github.com/Textualize/rich) that displays CoT activity in real time. Connects to the COP WebSocket and HTTP API to show entities, connection status, data sources, and an activity feed.

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
┌══════════════════════════════════════════════════════════════════════════╗
║  ◆ COMMON OPERATING PICTURE                                            ║
║  COP: ws://localhost:8080  │  Entities: 5  │  Deltas: 42  │  Uptime   ║
╠══════════════════════════════════════╤═══════════════════════════════════╣
│ CoT Entities (5)                     │ Status                           │
│ UID          TYPE       LAT   LON Src│ ✓ WebSocket                     │
│ ALPHA-1      a-f-G-U-C  38.9 -77 PEAT│ ✓ HTTP API                     │
│ TAK-2        a-h-G-U-C  39.1 -94 UDP │                                 │
│ WP-3         b-m-p-w    39.1 -76 POST│ Deltas/min  8.4                │
│                                      │                                  │
│                                      │ ── Sources ──                   │
│                                      │ ◆ PEAT ████████ 28              │
│                                      │ ◈ UDP  ███ 10                   │
│                                      │ ● POST █ 4                      │
│                                      │                                  │
│                                      │ ── Types ──                     │
│                                      │ a-f  ████ 4                     │
│                                      │ b-m  █ 1                        │
├──────────────────────────────────────┴──────────────────────────────────┤
│ Activity Feed (42)                                                      │
│ 09:15:32  ✦ NEW   ◆ PEAT  a-f-G-U-C  uid=ALPHA-1  38.8977°N 77.0365°W│
│ 09:15:33  ✦ NEW   ◈ UDP   a-h-G-U-C  uid=TAK-2    39.1000°N 94.5000°W│
│ 09:15:35  ◀ RECV  ◆ PEAT  a-f-G-U-C  uid=ALPHA-1  38.9000°N 77.0300°W│
│ 09:15:35  ▶ CAST  ● POST  uid=WP-3  changed: lat, lon                 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Panels:**

- **Header** — COP endpoint, total entity count, delta count, uptime
- **CoT Entities** — Table of tracked entities with columns: UID, CoT type, lat, lon, **source** (`PEAT`, `UDP`, `POST`), last seen timestamp. Sorted most-recently-updated first. Color-coded by affiliation (green = friendly `a-f`, red = hostile `a-h`, yellow = neutral `a-n`, dim = unknown `a-u`, cyan = markers `b-m`)
- **Status** — Live connection indicators (WebSocket + HTTP API), delta rate, **source breakdown** showing how many deltas arrived from each source, and **type breakdown** bar chart by CoT type prefix
- **Activity Feed** — Rolling log of the last 20 events with direction indicators (`✦ NEW`, `◀ RECV`, `▶ CAST`) and color-coded source tags (`◆ PEAT`, `◈ UDP`, `● POST`)

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

# Terminal 2 — CoT bridge (auto-discovers PeatLink via mDNS + listens on UDP multicast)
node cotBridge.js

# Terminal 3 — Live monitor
python3 cop-monitor.py
```

The bridge will automatically find any PeatLink server on the network. If PeatLink isn't running yet, the bridge retries every 3 seconds until it appears.

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

All three events will appear in the monitor's activity feed in real time. If the bridge is running, they will also appear on the PeatLink map.

### Simulating UDP Multicast

To test multicast UDP ingestion without a real ATAK device, send a CoT packet with `socat` or `netcat`:

```bash
# Using socat (recommended)
echo '<event uid="ATAK-SIM" type="a-f-G-U-C" how="m-g"
    time="2026-04-07T12:00:00Z" start="2026-04-07T12:00:00Z"
    stale="2026-04-07T13:00:00Z">
  <point lat="38.9072" lon="-77.0369" hae="0" ce="10" le="10"/>
</event>' | socat - UDP4-DATAGRAM:239.2.3.1:6969

# Or using Python
python3 -c "
import socket, struct
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
s.sendto(b'''<event uid=\"ATAK-SIM\" type=\"a-f-G-U-C\" how=\"m-g\"
    time=\"2026-04-07T12:00:00Z\" start=\"2026-04-07T12:00:00Z\"
    stale=\"2026-04-07T13:00:00Z\">
  <point lat=\"38.9072\" lon=\"-77.0369\" hae=\"0\" ce=\"10\" le=\"10\"/>
</event>''', ('239.2.3.1', 6969))
"
```

The entity will appear in the TUI tagged as `UDP` source.

---

## CoT Type Reference

Standard Cursor on Target type strings used throughout the system:

| Prefix | Affiliation | Examples | TUI Color |
|---|---|---|---|
| `a-f` | Friendly | `a-f-G-U-C` (ground unit), `a-f-A` (air) | Green |
| `a-h` | Hostile | `a-h-G-U-C` (hostile ground) | Red |
| `a-n` | Neutral | `a-n-G` (neutral ground) | Yellow |
| `a-u` | Unknown | `a-u-G` (unknown ground) | Dim |
| `b-m` | Marker/Bits | `b-m-p-w` (waypoint), `b-m-p-s-m` (marker), `b-m-p-s-p-i` (info) | Cyan |

The bridge maps marker CoT types to PeatLink icons:

| CoT Type | PeatLink Icon |
|---|---|
| `b-m-p-w` | `waypoint` |
| `b-m-p-s-p-i` | `info` |
| All other `b-*` | `rally` |

---

## Troubleshooting

### Bridge can't find PeatLink via mDNS

- Verify PeatLink is running and advertising: check its logs for `mDNS: advertising _peatlink._tcp on port 8090`
- Ensure both machines are on the same subnet (mDNS uses link-local multicast `224.0.0.251`)
- On macOS, mDNS works out of the box. On Linux, ensure `avahi-daemon` is running
- Try setting `PEAT_WS_URL` explicitly as a workaround: `PEAT_WS_URL=ws://192.168.1.50:8090/ws node cotBridge.js`
- Increase discovery timeout: `MDNS_TIMEOUT_MS=30000 node cotBridge.js`

### UDP multicast not receiving data

- Verify the multicast group is correct for your TAK setup (default: `239.2.3.1:6969`)
- Check your firewall allows UDP on the multicast port
- On some systems, you may need to specify the network interface. The bridge binds to `INADDR_ANY` by default
- Test with the `socat` command in the "Simulating UDP Multicast" section above

### Monitor shows "Waiting for CoT data"

- Verify the COP server is running (`node index.js` should print "CoT microservice running on port 3000")
- Check the WebSocket connection indicator in the Status panel — if it's spinning, the monitor can't reach `ws://localhost:8080`
- Use `--ws` and `--http` flags if the COP server is on a different host

### "ERR_REQUIRE_ESM" error

If you see this error, a dependency may have upgraded to ESM-only. The project uses CommonJS. Check that `uuid` is not imported (it was replaced with `crypto.randomUUID()` in Node 20+).

### Bridge logs "POST contact to COP failed"

The COP server isn't reachable. Make sure `node index.js` is running and the `COP_HTTP_URL` is correct.
