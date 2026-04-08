#!/usr/bin/env python3
"""
COP Monitor — Live TUI for Common Operating Picture
Real-time dashboard showing CoT entities received and broadcast.

Requirements: pip install rich websocket-client

Usage:
    ./cop-monitor.py
    ./cop-monitor.py --ws ws://10.0.0.5:8080 --http http://10.0.0.5:3000
"""

import json
import threading
import time
import argparse
import signal
import sys
import urllib.request
import urllib.error
from datetime import datetime

try:
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.console import Console, ConsoleOptions, RenderResult
    from rich import box
except ImportError:
    print("\n  This tool requires the 'rich' library:\n")
    print("    pip install rich\n")
    sys.exit(1)

try:
    import websocket
except ImportError:
    print("\n  This tool requires the 'websocket-client' library:\n")
    print("    pip install websocket-client\n")
    sys.exit(1)


# ── Style constants ─────────────────────────────────────────────────────────

COT_TYPE_STYLE = {
    "a-f": "green",         # friendly
    "a-h": "red",           # hostile
    "a-n": "yellow",        # neutral
    "a-u": "dim",           # unknown
    "b-m": "cyan",          # marker / bits
}

SOURCE_STYLE = {
    "peatlink":      ("bold magenta", "\u25c6 PEAT"),
    "udp-multicast": ("bold blue",    "\u25c8 UDP "),
    "direct":        ("bold white",   "\u25cf POST"),
}

_CONNECTED = "[bold green]\u2713[/]"   # checkmark
_WAITING   = "[bold yellow]\u25cf[/]"  # filled circle
_OFFLINE   = "[bold red]\u2717[/]"     # x mark

SPINNER_FRAMES = ["\u25d0", "\u25d3", "\u25d1", "\u25d2"]

DIRECTION_RECV = "[bold cyan]\u25c0 RECV[/]"
DIRECTION_CAST = "[bold magenta]\u25b6 CAST[/]"
DIRECTION_NEW  = "[bold green]\u2726 NEW [/]"


# ── Monitor ─────────────────────────────────────────────────────────────────

class CopMonitor:
    """Connects to COP WebSocket and renders a live dashboard."""

    def __init__(self, ws_url, http_url):
        self.ws_url   = ws_url
        self.http_url = http_url

        self.start_time  = datetime.now()
        self.entities    = {}       # uid -> {type, lat, lon, hae, ce, last_seen, source}
        self.activity    = []       # list of rich-markup strings
        self.delta_count = 0
        self.ws_connected = False
        self.http_ok      = False
        self.ws_clients   = "?"
        self.aborted      = False
        self.lock         = threading.Lock()

        # Source counters
        self.source_counts = {"peatlink": 0, "udp-multicast": 0, "direct": 0}

    # ── Rich renderable protocol ────────────────────────────────────────────

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield self._build_layout()

    # ── Activity logging ────────────────────────────────────────────────────

    def _log(self, line):
        ts = datetime.now().strftime("%H:%M:%S")
        self.activity.append(f"[dim]{ts}[/]  {line}")
        if len(self.activity) > 200:
            self.activity = self.activity[-200:]

    # ── Layout builders ─────────────────────────────────────────────────────

    def _build_layout(self):
        layout = Layout()
        layout.split_column(
            Layout(name="header",   size=4),
            Layout(name="middle",   size=15),
            Layout(name="activity"),
        )
        layout["middle"].split_row(
            Layout(name="entities", ratio=2),
            Layout(name="status",   ratio=1),
        )

        layout["header"].update(self._header_panel())
        layout["entities"].update(self._entities_panel())
        layout["status"].update(self._status_panel())
        layout["activity"].update(self._activity_panel())
        return layout

    def _header_panel(self):
        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split(".")[0]

        t = Text()
        t.append("  \u25c6 COMMON OPERATING PICTURE\n", style="bold cyan")
        t.append("  COP: ", style="dim")
        t.append(self.ws_url, style="bold white")
        t.append("  \u2502  Entities: ", style="dim")
        t.append(str(len(self.entities)), style="bold white")
        t.append("  \u2502  Deltas: ", style="dim")
        t.append(str(self.delta_count), style="bold white")
        t.append("  \u2502  Uptime: ", style="dim")
        t.append(elapsed_str, style="white")
        return Panel(t, box=box.HEAVY, style="cyan")

    def _entities_panel(self):
        tbl = Table(box=None, show_header=True, padding=(0, 1), expand=True)
        tbl.add_column("UID",      ratio=2, no_wrap=True, style="bold")
        tbl.add_column("Type",     ratio=2, no_wrap=True)
        tbl.add_column("Lat",      width=10, justify="right", no_wrap=True)
        tbl.add_column("Lon",      width=10, justify="right", no_wrap=True)
        tbl.add_column("Src",      width=6,  justify="center", no_wrap=True)
        tbl.add_column("Last Seen", width=10, justify="right", no_wrap=True)

        with self.lock:
            sorted_ents = sorted(
                self.entities.items(),
                key=lambda kv: kv[1].get("last_seen", ""),
                reverse=True,
            )

        if not sorted_ents:
            tbl.add_row(
                Text("Waiting for CoT data\u2026", style="dim italic"),
                "", "", "", "", "",
            )
        else:
            for uid, ent in sorted_ents[:20]:
                cot_type = ent.get("type", "unknown")
                style = _style_for_type(cot_type)

                short_uid = uid[:16] + "\u2026" if len(uid) > 16 else uid
                lat_str = f"{float(ent.get('lat', 0)):.4f}"
                lon_str = f"{float(ent.get('lon', 0)):.4f}"
                seen = ent.get("last_seen", "")
                source = ent.get("source", "?")

                src_style, src_label = SOURCE_STYLE.get(source, ("dim", source[:4]))

                tbl.add_row(
                    Text(short_uid, style="bold"),
                    Text(cot_type, style=style),
                    Text(lat_str, style="white"),
                    Text(lon_str, style="white"),
                    Text(src_label, style=src_style),
                    Text(seen, style="dim"),
                )

        return Panel(
            tbl,
            title=f"[bold]CoT Entities ({len(self.entities)})[/]",
            border_style="blue",
            box=box.ROUNDED,
        )

    def _status_panel(self):
        t = Text()

        # WebSocket
        if self.ws_connected:
            t.append_text(Text.from_markup(f"  {_CONNECTED}"))
            t.append(" WebSocket\n", style="green")
        else:
            frame = SPINNER_FRAMES[int(time.time() * 4) % len(SPINNER_FRAMES)]
            t.append(f"  {frame}", style="bold yellow")
            t.append(" WebSocket\n", style="yellow")

        # HTTP API
        if self.http_ok:
            t.append_text(Text.from_markup(f"  {_CONNECTED}"))
            t.append(" HTTP API\n", style="green")
        else:
            t.append_text(Text.from_markup(f"  {_OFFLINE}"))
            t.append(" HTTP API\n", style="red")

        t.append("\n")

        # Stats
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.delta_count / max(elapsed / 60, 0.01)

        t.append("  Deltas/min  ", style="dim")
        t.append(f"{rate:.1f}\n", style="bold white")
        t.append("  Total       ", style="dim")
        t.append(f"{self.delta_count}\n", style="bold white")
        t.append("  Entities    ", style="dim")
        t.append(f"{len(self.entities)}\n", style="bold white")

        # Source breakdown
        t.append("\n")
        t.append("  ── Sources ──\n", style="dim")
        for src_key, count in sorted(self.source_counts.items()):
            if count == 0:
                continue
            src_style, src_label = SOURCE_STYLE.get(src_key, ("dim", src_key))
            bar = "\u2588" * min(count, 12)
            t.append(f"  {src_label} ", style=src_style)
            t.append(f"{bar} ", style=src_style)
            t.append(f"{count}\n", style=f"bold {src_style}")

        # Type breakdown
        type_counts = {}
        with self.lock:
            for ent in self.entities.values():
                prefix = ent.get("type", "?")[:3]
                type_counts[prefix] = type_counts.get(prefix, 0) + 1

        if type_counts:
            t.append("\n")
            t.append("  ── Types ──\n", style="dim")
            for prefix, count in sorted(type_counts.items()):
                style = _style_for_type(prefix)
                t.append(f"  {prefix:<6}", style=style)
                bar = "\u2588" * min(count, 12)
                t.append(f" {bar} ", style=style)
                t.append(f"{count}\n", style=f"bold {style}")

        return Panel(
            t,
            title="[bold]Status[/]",
            border_style="green" if self.ws_connected else "yellow",
            box=box.ROUNDED,
        )

    def _activity_panel(self):
        visible = self.activity[-20:]
        if not visible:
            content = Text("  Waiting for CoT activity\u2026", style="dim italic")
        else:
            content = Text()
            for line in visible:
                content.append_text(Text.from_markup(f"  {line}\n"))

        return Panel(
            content,
            title=f"[bold]Activity Feed ({self.delta_count})[/]",
            border_style="yellow",
            box=box.ROUNDED,
        )

    # ── Source tag helper ───────────────────────────────────────────────────

    def _source_tag(self, source):
        style, label = SOURCE_STYLE.get(source, ("dim", source[:6]))
        return f"[{style}]{label}[/]"

    # ── WebSocket handler ───────────────────────────────────────────────────

    def _ws_on_open(self, ws):
        self.ws_connected = True
        self._log("[bold green]WebSocket connected[/]")

    def _ws_on_close(self, ws, close_status, close_msg):
        self.ws_connected = False
        self._log("[bold red]WebSocket disconnected[/]")

    def _ws_on_error(self, ws, error):
        self._log(f"[bold red]WS error:[/] {error}")

    def _ws_on_message(self, ws, raw):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Skip the welcome message
        if "message" in data and "Welcome" in str(data.get("message", "")):
            self._log(f"{DIRECTION_RECV}  [dim]Welcome from COP[/]")
            return

        self.delta_count += 1
        now_str = datetime.now().strftime("%H:%M:%S")
        source = data.get("_source", "direct")

        # Track source
        if source in self.source_counts:
            self.source_counts[source] += 1
        else:
            self.source_counts[source] = 1

        src_tag = self._source_tag(source)

        # New entity (full object from cotStore)
        if "event" in data and "$" in data.get("event", {}):
            attrs = data["event"]["$"]
            point = {}
            if "point" in data["event"]:
                pts = data["event"]["point"]
                point = pts[0]["$"] if isinstance(pts, list) and pts else pts.get("$", {})

            uid      = attrs.get("uid", "unknown")
            cot_type = attrs.get("type", "unknown")
            lat      = point.get("lat", "0")
            lon      = point.get("lon", "0")
            hae      = point.get("hae", "0")
            ce       = point.get("ce", "999999")

            is_new = uid not in self.entities
            with self.lock:
                self.entities[uid] = {
                    "type": cot_type, "lat": lat, "lon": lon,
                    "hae": hae, "ce": ce, "last_seen": now_str,
                    "source": source,
                }

            style = _style_for_type(cot_type)
            short = uid[:12]
            tag = DIRECTION_NEW if is_new else DIRECTION_RECV
            self._log(
                f"{tag}  {src_tag}  [{style}]{cot_type}[/]  "
                f"uid=[bold]{short}[/]  "
                f"{float(lat):.4f}\u00b0N {abs(float(lon)):.4f}\u00b0W"
            )

        # Update delta (partial changes)
        elif "uid" in data and "changes" in data:
            uid     = data["uid"]
            changes = data["changes"]

            with self.lock:
                if uid in self.entities:
                    self.entities[uid].update(changes)
                    self.entities[uid]["last_seen"] = now_str
                    self.entities[uid]["source"] = source
                else:
                    self.entities[uid] = {**changes, "last_seen": now_str, "source": source}

            changed_keys = ", ".join(changes.keys())
            short = uid[:12] if uid else "?"
            self._log(
                f"{DIRECTION_CAST}  {src_tag}  uid=[bold]{short}[/]  "
                f"[dim]changed:[/] {changed_keys}"
            )

    # ── HTTP poller (syncs full entity state) ───────────────────────────────

    def _poll_http(self):
        while not self.aborted:
            try:
                req = urllib.request.Request(f"{self.http_url}/cot")
                with urllib.request.urlopen(req, timeout=3) as resp:
                    entities = json.loads(resp.read())
                    self.http_ok = True

                    now_str = datetime.now().strftime("%H:%M:%S")
                    with self.lock:
                        for ent in entities:
                            attrs = ent.get("event", {}).get("$", {})
                            pts = ent.get("event", {}).get("point", [])
                            point = pts[0].get("$", {}) if isinstance(pts, list) and pts else {}
                            uid = attrs.get("uid")
                            if uid and uid not in self.entities:
                                self.entities[uid] = {
                                    "type": attrs.get("type", "unknown"),
                                    "lat": point.get("lat", "0"),
                                    "lon": point.get("lon", "0"),
                                    "hae": point.get("hae", "0"),
                                    "ce": point.get("ce", "999999"),
                                    "last_seen": now_str,
                                    "source": "sync",
                                }
            except Exception:
                self.http_ok = False

            time.sleep(5)

    # ── WebSocket connection loop (auto-reconnect) ──────────────────────────

    def _ws_loop(self):
        while not self.aborted:
            try:
                ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._ws_on_open,
                    on_message=self._ws_on_message,
                    on_close=self._ws_on_close,
                    on_error=self._ws_on_error,
                )
                ws.run_forever(ping_interval=10, ping_timeout=5)
            except Exception as e:
                self._log(f"[bold red]WS reconnect error:[/] {e}")

            self.ws_connected = False
            if not self.aborted:
                self._log("[dim]Reconnecting in 3s\u2026[/]")
                time.sleep(3)

    # ── Main entry ──────────────────────────────────────────────────────────

    def run(self):
        signal.signal(signal.SIGINT, lambda *_: self._shutdown())

        # Start background threads
        ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        ws_thread.start()

        http_thread = threading.Thread(target=self._poll_http, daemon=True)
        http_thread.start()

        self._log("[bold cyan]COP Monitor started[/]")
        self._log(f"[dim]WebSocket:[/] {self.ws_url}")
        self._log(f"[dim]HTTP API:[/]  {self.http_url}")

        try:
            with Live(self, refresh_per_second=4, screen=True):
                while not self.aborted:
                    time.sleep(0.25)
        except KeyboardInterrupt:
            pass

    def _shutdown(self):
        self.aborted = True


# ── Helpers ─────────────────────────────────────────────────────────────────

def _style_for_type(cot_type):
    """Return a Rich style string based on CoT type prefix."""
    t = str(cot_type)
    for prefix, style in COT_TYPE_STYLE.items():
        if t.startswith(prefix):
            return style
    return "white"


# ── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="COP Monitor — Live CoT Dashboard")
    parser.add_argument("--ws",   default="ws://localhost:8080",     help="COP WebSocket URL")
    parser.add_argument("--http", default="http://localhost:3000",   help="COP HTTP API URL")
    args = parser.parse_args()

    monitor = CopMonitor(ws_url=args.ws, http_url=args.http)
    monitor.run()


if __name__ == "__main__":
    main()
