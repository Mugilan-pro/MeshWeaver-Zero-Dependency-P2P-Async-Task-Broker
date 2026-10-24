"""
Terminal dashboard for MeshWeaver nodes.

Visualizes node health, CPU and memory usage meters, gossip rounds,
and cluster events in the terminal.
Works with the rich library when installed, or falls back to standard ANSI boxes.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from gossip.models import NodeMetrics, PeerHealthState, PeerNodeRecord
from gossip.monitor import GossipMonitor

try:
    import rich
    from rich.align import Align
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    rich = None
    _HAS_RICH = False


class DashboardEventLogger:
    """Stores the last N events for display in the dashboard."""

    def __init__(self, max_events: int = 10) -> None:
        self.max_events = max_events
        self.events: List[Tuple[str, str, str]] = []

    def log(self, level: str, message: str) -> None:
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self.events.append((ts, level.upper(), message))
        if len(self.events) > self.max_events:
            self.events.pop(0)

    def get_recent(self) -> List[Tuple[str, str, str]]:
        return list(self.events)


def _render_bar(percent: float, width: int = 10) -> str:
    """Renders a progress bar: [████░░░░░░] 40.0%"""
    clamped = max(0.0, min(100.0, float(percent)))
    filled = int(round((clamped / 100.0) * width))
    empty = width - filled
    return f"[{'█' * filled}{'░' * empty}] {clamped:5.1f}%"


class RichTerminalDashboard:
    """Renders real-time cluster status, peer metrics, and event logs."""

    def __init__(
        self,
        monitor: GossipMonitor,
        router: Optional[Any] = None,
        reliability: Optional[Any] = None,
        event_logger: Optional[DashboardEventLogger] = None,
    ) -> None:
        self.monitor = monitor
        self.router = router
        self.reliability = reliability
        self.event_logger = event_logger or DashboardEventLogger()
        self._console = Console() if _HAS_RICH else None

        self._wire_monitor_events()

    def _wire_monitor_events(self) -> None:
        orig_dropout = self.monitor.on_peer_dropout
        orig_recovered = self.monitor.on_peer_recovered
        orig_suspect = self.monitor.on_peer_suspect

        def _on_dropout(node_id: str) -> None:
            self.event_logger.log("ALERT", f"Lost heartbeat: {node_id} marked DEAD")
            if orig_dropout:
                orig_dropout(node_id)

        def _on_recovered(node_id: str) -> None:
            self.event_logger.log("INFO", f"Node {node_id} is back online")
            if orig_recovered:
                orig_recovered(node_id)

        def _on_suspect(node_id: str) -> None:
            self.event_logger.log("WARN", f"Missed heartbeat from {node_id} (SUSPECT)")
            if orig_suspect:
                orig_suspect(node_id)

        self.monitor.on_peer_dropout = _on_dropout
        self.monitor.on_peer_recovered = _on_recovered
        self.monitor.on_peer_suspect = _on_suspect

    # -------------------------------------------------------------------------
    # Rich UI Rendering (when rich is installed)
    # -------------------------------------------------------------------------

    def _build_rich_table(self) -> Table:
        table = Table(title="Mesh Nodes & Telemetry", expand=True)
        table.add_column("Node ID", style="cyan", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Endpoint", style="dim")
        table.add_column("CPU Usage", justify="left")
        table.add_column("RAM Usage", justify="left")
        table.add_column("Tasks (Act/Done/Fail)", justify="center")
        table.add_column("Load Score", justify="right")
        table.add_column("Last Heartbeat", justify="right")

        # Local node
        local_metrics = self.monitor.metrics_collector.collect()
        table.add_row(
            f"[bold green]★ {self.monitor.local_node_id} (Local)[/bold green]",
            "[bold green]HEALTHY[/bold green]",
            f"{self.monitor.host}:{self.monitor.port}",
            _render_bar(local_metrics.cpu_percent),
            _render_bar(local_metrics.ram_percent),
            f"{local_metrics.active_tasks} / {local_metrics.completed_tasks} / {local_metrics.failed_tasks}",
            f"{local_metrics.composite_score:.1f}",
            "0.0s (self)",
        )

        # Peer nodes
        peers = sorted(self.monitor.get_all_peers(), key=lambda p: p.node_id)
        for peer in peers:
            status_text = {
                PeerHealthState.HEALTHY: "[bold green]HEALTHY[/bold green]",
                PeerHealthState.SUSPECT: "[bold yellow]SUSPECT[/bold yellow]",
                PeerHealthState.DEAD: "[bold red]DEAD[/bold red]",
            }.get(peer.health_state, "[dim]UNKNOWN[/dim]")

            m = peer.metrics
            elapsed = peer.seconds_since_last_heartbeat
            elapsed_str = f"{elapsed:.1f}s ago" if peer.is_alive else "[bold red]DEAD[/bold red]"

            table.add_row(
                f"  {peer.node_id}",
                status_text,
                f"{peer.host}:{peer.port}",
                _render_bar(m.cpu_percent),
                _render_bar(m.ram_percent),
                f"{m.active_tasks} / {m.completed_tasks} / {m.failed_tasks}",
                f"{m.composite_score:.1f}",
                elapsed_str,
            )

        return table

    def _build_rich_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=8),
        )

        local_metrics = self.monitor.metrics_collector.collect()
        uptime_fmt = str(datetime.timedelta(seconds=int(local_metrics.uptime_seconds)))
        healthy_count = self.monitor.healthy_peer_count + 1
        total_count = self.monitor.peer_count + 1

        header_text = Text.assemble(
            ("MeshWeaver P2P Task Broker ", "bold cyan"),
            ("• ", "white"),
            (f"Node: {self.monitor.local_node_id} ", "bold yellow"),
            (f"({self.monitor.host}:{self.monitor.port}) ", "dim"),
            ("• ", "white"),
            (f"Cluster: {healthy_count}/{total_count} Healthy ", "bold green" if self.monitor.dead_peer_count == 0 else "bold red"),
            ("• ", "white"),
            (f"Uptime: {uptime_fmt}", "dim"),
        )
        layout["header"].update(Panel(Align.center(header_text), style="blue"))
        layout["main"].update(Panel(self._build_rich_table(), border_style="cyan"))

        layout["footer"].split_row(
            Layout(name="stats", ratio=1),
            Layout(name="events", ratio=2),
        )

        stats = self.monitor.stats
        stats_content = (
            f"[bold]Gossip Protocol (Week 2):[/bold]\n"
            f"• Rounds: [cyan]{stats['gossip_rounds']}[/cyan] (every {self.monitor.gossip_interval}s)\n"
            f"• Fanout: [cyan]{self.monitor.fanout}[/cyan] peers per round\n"
            f"• Messages: [green]{stats['messages_sent']} sent[/green] / [green]{stats['messages_received']} received[/green]\n"
            f"• Dropouts: [red]{stats['dropouts_detected']}[/red] | Recoveries: [green]{stats['recoveries_detected']}[/green]"
        )
        layout["footer"]["stats"].update(Panel(stats_content, title="Gossip Telemetry", border_style="magenta"))

        events = self.event_logger.get_recent()
        event_lines = []
        for ts, lvl, msg in events[-5:]:
            color = {"INFO": "green", "GOSSIP": "cyan", "WARN": "yellow", "ALERT": "red"}.get(lvl, "white")
            event_lines.append(f"[dim]{ts}[/dim] [{color}][{lvl}][/{color}] {msg}")
        if not event_lines:
            event_lines.append("[dim]No events logged yet.[/dim]")

        layout["footer"]["events"].update(Panel("\n".join(event_lines), title="Cluster Event Log", border_style="yellow"))
        return layout

    # -------------------------------------------------------------------------
    # Fallback Terminal Renderer (pure Python standard output)
    # -------------------------------------------------------------------------

    def _render_fallback_snapshot(self) -> str:
        lines = []
        c_cyan = "\033[96m"
        c_green = "\033[92m"
        c_yellow = "\033[93m"
        c_red = "\033[91m"
        c_bold = "\033[1m"
        c_dim = "\033[2m"
        c_reset = "\033[0m"

        w = 100
        lines.append(f"{c_cyan}╔{'═' * (w - 2)}╗{c_reset}")
        title = " MeshWeaver P2P Task Broker — Cluster Monitor "
        lines.append(f"{c_cyan}║{c_bold}{title.center(w - 2)}{c_reset}{c_cyan}║{c_reset}")
        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        local_metrics = self.monitor.metrics_collector.collect()
        uptime_fmt = str(datetime.timedelta(seconds=int(local_metrics.uptime_seconds)))
        meta = (
            f" Node: {self.monitor.local_node_id} ({self.monitor.host}:{self.monitor.port}) "
            f"| Health: {self.monitor.healthy_peer_count + 1}/{self.monitor.peer_count + 1} Healthy "
            f"| Uptime: {uptime_fmt} "
        )
        lines.append(f"{c_cyan}║{c_reset}{meta.ljust(w - 2)}{c_cyan}║{c_reset}")
        lines.append(f"{c_cyan}╠{'═' * 18}╦{'═' * 11}╦{'═' * 18}╦{'═' * 18}╦{'═' * 18}╦{'═' * 11}╣{c_reset}")

        hdr = (
            f"{c_cyan}║{c_reset} {c_bold}{'Node ID':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Status':9}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Endpoint':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'CPU Usage':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'RAM Usage':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Heartbeat':9}{c_reset} {c_cyan}║{c_reset}"
        )
        lines.append(hdr)
        lines.append(f"{c_cyan}╠{'═' * 18}╬{'═' * 11}╬{'═' * 18}╬{'═' * 18}╬{'═' * 18}╬{'═' * 11}╣{c_reset}")

        # Local node row
        local_cpu_bar = _render_bar(local_metrics.cpu_percent, width=8)
        local_ram_bar = _render_bar(local_metrics.ram_percent, width=8)
        row_local = (
            f"{c_cyan}║{c_reset} {c_green}★ {self.monitor.local_node_id[:14]:14}{c_reset} "
            f"{c_cyan}║{c_reset} {c_green}{'HEALTHY':9}{c_reset} "
            f"{c_cyan}║{c_reset} {f'{self.monitor.host}:{self.monitor.port}':16} "
            f"{c_cyan}║{c_reset} {local_cpu_bar:16} "
            f"{c_cyan}║{c_reset} {local_ram_bar:16} "
            f"{c_cyan}║{c_reset} {'0.0s self':9} {c_cyan}║{c_reset}"
        )
        lines.append(row_local)

        # Peer rows
        peers = sorted(self.monitor.get_all_peers(), key=lambda p: p.node_id)
        for peer in peers:
            status_col = {
                PeerHealthState.HEALTHY: f"{c_green}{'HEALTHY':9}{c_reset}",
                PeerHealthState.SUSPECT: f"{c_yellow}{'SUSPECT':9}{c_reset}",
                PeerHealthState.DEAD: f"{c_red}{'DEAD':9}{c_reset}",
            }.get(peer.health_state, f"{'UNKNOWN':9}")

            m = peer.metrics
            cpu_bar = _render_bar(m.cpu_percent, width=8)
            ram_bar = _render_bar(m.ram_percent, width=8)
            elapsed = peer.seconds_since_last_heartbeat
            elapsed_str = f"{elapsed:.1f}s" if peer.is_alive else "DEAD"

            row_peer = (
                f"{c_cyan}║{c_reset} {peer.node_id[:16]:16} "
                f"{c_cyan}║{c_reset} {status_col} "
                f"{c_cyan}║{c_reset} {f'{peer.host}:{peer.port}':16} "
                f"{c_cyan}║{c_reset} {cpu_bar:16} "
                f"{c_cyan}║{c_reset} {ram_bar:16} "
                f"{c_cyan}║{c_reset} {elapsed_str:9} {c_cyan}║{c_reset}"
            )
            lines.append(row_peer)

        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        # Gossip stats
        stats = self.monitor.stats
        stats_line = (
            f" [Gossip Telemetry] Rounds: {stats['gossip_rounds']} (interval {self.monitor.gossip_interval}s) "
            f"| Sent: {stats['messages_sent']} | Recv: {stats['messages_received']} "
            f"| Dropouts: {stats['dropouts_detected']} | Recoveries: {stats['recoveries_detected']}"
        )
        lines.append(f"{c_cyan}║{c_reset}{stats_line.ljust(w - 2)}{c_cyan}║{c_reset}")
        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        # Events
        lines.append(f"{c_cyan}║{c_reset} {c_bold}[Cluster Event Log]{c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        events = self.event_logger.get_recent()
        if not events:
            lines.append(f"{c_cyan}║{c_reset} {c_dim}(No events logged yet){c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        else:
            for ts, lvl, msg in events[-3:]:
                color = {"INFO": c_green, "GOSSIP": c_cyan, "WARN": c_yellow, "ALERT": c_red}.get(lvl, "")
                entry = f" {c_dim}{ts}{c_reset} {color}[{lvl}]{c_reset} {msg}"
                lines.append(f"{c_cyan}║{c_reset}{entry}".ljust(w + 14) + f"{c_cyan}║{c_reset}")

        lines.append(f"{c_cyan}╚{'═' * (w - 2)}╝{c_reset}")
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------

    def render_snapshot(self) -> str:
        """Returns the current dashboard as a string."""
        if _HAS_RICH and self._console:
            with self._console.capture() as capture:
                self._console.print(self._build_rich_layout())
            return capture.get()
        return self._render_fallback_snapshot()

    def print_snapshot(self) -> None:
        """Prints the dashboard snapshot to stdout."""
        if _HAS_RICH and self._console:
            self._console.print(self._build_rich_layout())
        else:
            print(self._render_fallback_snapshot())

    async def run_live(self, refresh_seconds: float = 1.0, max_ticks: Optional[int] = None) -> None:
        """Runs the live-updating dashboard in the terminal."""
        tick = 0
        if _HAS_RICH and self._console:
            with Live(self._build_rich_layout(), console=self._console, refresh_per_second=int(1.0 / refresh_seconds)) as live:
                while self.monitor.is_running:
                    await asyncio.sleep(refresh_seconds)
                    live.update(self._build_rich_layout())
                    tick += 1
                    if max_ticks and tick >= max_ticks:
                        break
        else:
            while self.monitor.is_running:
                sys.stdout.write("\033[H\033[2J")
                sys.stdout.flush()
                print(self._render_fallback_snapshot())
                await asyncio.sleep(refresh_seconds)
                tick += 1
                if max_ticks and tick >= max_ticks:
                    break


if __name__ == "__main__":
    async def _main():
        print("Starting MeshWeaver Standalone Dashboard...")
        monitor = GossipMonitor(local_node_id="local_node_0", host="127.0.0.1", port=8000)

        p1 = monitor.add_peer("worker_alpha", port=8001)
        p1.metrics.cpu_percent = 22.5
        p1.metrics.ram_percent = 45.0
        p1.metrics.active_tasks = 1

        p2 = monitor.add_peer("worker_beta", port=8002)
        p2.metrics.cpu_percent = 78.4
        p2.metrics.ram_percent = 82.1
        p2.metrics.active_tasks = 4

        p3 = monitor.add_peer("worker_gamma", port=8003)
        p3.health_state = PeerHealthState.DEAD
        p3.last_heartbeat_time = time.time() - 25.0

        dash = RichTerminalDashboard(monitor)
        dash.event_logger.log("INFO", "Cluster started with 3 workers")
        dash.event_logger.log("WARN", "High CPU on worker_beta (78.4%)")
        dash.event_logger.log("ALERT", "worker_gamma heartbeat timed out (marked DEAD)")

        dash.print_snapshot()

    asyncio.run(_main())
