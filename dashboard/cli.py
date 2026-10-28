"""
Terminal dashboard for MeshWeaver nodes.

Visualizes:
1. Real-Time Mesh Topology & Inter-Node Link Graph (Week 4 Deliverable)
2. Live Task Execution State (Active Leases, Retries, DLQ) (Week 4 Deliverable)
3. Cryptographic Security & TLS Transport Status (Week 4 Deliverable)
4. Hardware Telemetry & Health Meters (CPU%, RAM%, Load Score) (Weeks 1 & 2)
5. Gossip Telemetry Counters & Cluster Event Log

Works with the rich library when installed, or falls back to an ANSI/Unicode
high-fidelity box-drawing renderer with zero external dependencies.
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
    import rich  # type: ignore
    from rich.align import Align  # type: ignore
    from rich.console import Console  # type: ignore
    from rich.layout import Layout  # type: ignore
    from rich.live import Live  # type: ignore
    from rich.panel import Panel  # type: ignore
    from rich.progress_bar import ProgressBar  # type: ignore
    from rich.table import Table  # type: ignore
    from rich.text import Text  # type: ignore
    _HAS_RICH = True
except ImportError:
    rich = None
    _HAS_RICH = False


class DashboardEventLogger:
    """Stores recent cluster events for real-time visualization."""

    def __init__(self, max_events: int = 15) -> None:
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
    """
    Real-time Terminal Dashboard for MeshWeaver.
    Displays mesh topology, task execution lifecycle, node telemetry, and cluster events.
    """

    def __init__(
        self,
        monitor: GossipMonitor,
        router: Optional[Any] = None,
        reliability: Optional[Any] = None,
        executor: Optional[Any] = None,
        security: Optional[Any] = None,
        event_logger: Optional[DashboardEventLogger] = None,
    ) -> None:
        self.monitor = monitor
        self.router = router
        self.reliability = reliability
        self.executor = executor
        self.security = security
        self.event_logger = event_logger or DashboardEventLogger()
        self._console = Console() if _HAS_RICH else None

        self._wire_monitor_events()

    def _wire_monitor_events(self) -> None:
        orig_dropout = self.monitor.on_peer_dropout
        orig_recovered = self.monitor.on_peer_recovered
        orig_suspect = self.monitor.on_peer_suspect

        def _on_dropout(node_id: str) -> None:
            self.event_logger.log("ALERT", f"Heartbeat lost: {node_id} marked DEAD. Initiating task re-route.")
            if orig_dropout:
                orig_dropout(node_id)

        def _on_recovered(node_id: str) -> None:
            self.event_logger.log("INFO", f"Node {node_id} returned online. Marked HEALTHY.")
            if orig_recovered:
                orig_recovered(node_id)

        def _on_suspect(node_id: str) -> None:
            self.event_logger.log("WARN", f"Node {node_id} missed heartbeat (flagged SUSPECT).")
            if orig_suspect:
                orig_suspect(node_id)

        self.monitor.on_peer_dropout = _on_dropout
        self.monitor.on_peer_recovered = _on_recovered
        self.monitor.on_peer_suspect = _on_suspect

        if self.reliability:
            orig_reassigned = self.reliability.on_task_reassigned
            orig_exhausted = self.reliability.on_task_exhausted

            def _on_reassigned(task: Any, old_worker: str, new_worker: str) -> None:
                self.event_logger.log(
                    "ROUTING",
                    f"Recovered '{task.name}' ({task.task_id[:8]}): {old_worker} -> {new_worker} (Attempt {task.retry_count + 1})",
                )
                if orig_reassigned:
                    orig_reassigned(task, old_worker, new_worker)

            def _on_exhausted(task: Any, reason: str) -> None:
                self.event_logger.log(
                    "ALERT",
                    f"Task '{task.name}' ({task.task_id[:8]}) moved to Dead-Letter Queue (retries exhausted)",
                )
                if orig_exhausted:
                    orig_exhausted(task, reason)

            self.reliability.on_task_reassigned = _on_reassigned
            self.reliability.on_task_exhausted = _on_exhausted

    # -------------------------------------------------------------------------
    # Rich UI Rendering (when rich library is installed)
    # -------------------------------------------------------------------------

    def _build_rich_telemetry_table(self) -> Table:
        table = Table(title="Cluster Nodes & Hardware Telemetry", expand=True)
        table.add_column("Node ID", style="cyan", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Security", justify="center")
        table.add_column("Endpoint", style="dim")
        table.add_column("CPU Usage", justify="left")
        table.add_column("RAM Usage", justify="left")
        table.add_column("Tasks (Act/Done/Fail)", justify="center")
        table.add_column("Load Score", justify="right")
        table.add_column("Heartbeat", justify="right")

        local_metrics = self.monitor.metrics_collector.collect()
        table.add_row(
            f"[bold green]★ {self.monitor.local_node_id} (Local)[/bold green]",
            "[bold green]HEALTHY[/bold green]",
            "[bold cyan]🔒 TLS + HMAC[/bold cyan]",
            f"{self.monitor.host}:{self.monitor.port}",
            _render_bar(local_metrics.cpu_percent),
            _render_bar(local_metrics.ram_percent),
            f"{local_metrics.active_tasks} / {local_metrics.completed_tasks} / {local_metrics.failed_tasks}",
            f"{local_metrics.composite_score:.1f}",
            "0.0s (self)",
        )

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
            tls_tag = "[bold cyan]🔒 TLS[/bold cyan]" if peer.is_tls else "[dim]🔓 Plain[/dim]"

            table.add_row(
                f"  {peer.node_id}",
                status_text,
                tls_tag,
                f"{peer.host}:{peer.port}",
                _render_bar(m.cpu_percent),
                _render_bar(m.ram_percent),
                f"{m.active_tasks} / {m.completed_tasks} / {m.failed_tasks}",
                f"{m.composite_score:.1f}",
                elapsed_str,
            )

        return table

    def _build_rich_task_state_table(self) -> Table:
        table = Table(title="Live Task Execution & Fault Recovery State", expand=True)
        table.add_column("Task ID", style="cyan", no_wrap=True)
        table.add_column("Workload Name", style="bold")
        table.add_column("Origin", style="dim")
        table.add_column("Assigned Worker", style="yellow")
        table.add_column("Status", justify="center")
        table.add_column("Retries", justify="center")
        table.add_column("Security", justify="center")
        table.add_column("Elapsed", justify="right")

        has_tasks = False
        if self.reliability:
            leases = self.reliability.get_active_leases()
            for lease in leases:
                has_tasks = True
                task = lease.task
                age = time.time() - lease.assigned_at
                status_badge = "[bold yellow]RUNNING[/bold yellow]"
                retry_badge = f"{task.retry_count}/{task.max_retries}"
                sec_badge = "[bold green]✓ Signed[/bold green]" if task.signature else "[dim]Unsigned[/dim]"
                table.add_row(
                    f"{task.task_id[:8]}...",
                    task.name,
                    task.origin_node_id or "local",
                    f"→ {lease.worker_node_id}",
                    status_badge,
                    retry_badge,
                    sec_badge,
                    f"{age:.1f}s",
                )

            # Dead letter queue
            dlq = self.reliability.get_dead_letter_tasks()
            for task, err in dlq[-3:]:
                has_tasks = True
                table.add_row(
                    f"[red]{task.task_id[:8]}...[/red]",
                    f"[red]{task.name}[/red]",
                    task.origin_node_id or "local",
                    "[bold red]DEAD-LETTER[/bold red]",
                    "[bold red]FAILED[/bold red]",
                    f"[bold red]{task.retry_count}/{task.max_retries}[/bold red]",
                    "[bold red]DLQ[/bold red]",
                    "—",
                )

        if not has_tasks:
            table.add_row("[dim]No active in-flight leases[/dim]", "—", "—", "—", "[green]IDLE[/green]", "0/0", "—", "—")

        return table

    def _build_rich_topology_panel(self) -> Panel:
        lines = []
        local_id = self.monitor.local_node_id
        peers = sorted(self.monitor.get_all_peers(), key=lambda p: p.node_id)

        lines.append(f"[bold cyan]★ Local Hub: {local_id} ({self.monitor.host}:{self.monitor.port})[/bold cyan] [bold green][ONLINE][/bold green]")
        lines.append("   Active P2P Mesh Edges & Link Latencies:")

        if not peers:
            lines.append("   [dim]No peer nodes connected. Mesh in standalone discovery mode.[/dim]")
        else:
            for peer in peers:
                status_color = "green" if peer.is_healthy else ("yellow" if peer.is_suspect else "red")
                icon = "🔒" if peer.is_tls else "🔓"
                rtt = f"{peer.round_trip_time_ms:.1f}ms" if peer.round_trip_time_ms > 0 else "< 2.0ms"
                lines.append(
                    f"   ├── {icon} [{status_color}]●[/{status_color}] [bold]{peer.node_id:14}[/bold] "
                    f"({peer.host}:{peer.port}) ──[ RTT: {rtt} | TLS: {peer.is_tls} | Status: [{status_color}]{peer.health_state.value}[/{status_color}] ]"
                )

        return Panel("\n".join(lines), title="Real-Time Mesh Topology", border_style="cyan")

    def _build_rich_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="topology", size=6),
            Layout(name="telemetry", ratio=1),
            Layout(name="tasks", size=7),
            Layout(name="footer", size=7),
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
            (f"Mesh: {healthy_count}/{total_count} Healthy ", "bold green" if self.monitor.dead_peer_count == 0 else "bold red"),
            ("• ", "white"),
            ("Security: 🔒 TLS + HMAC-SHA256 ", "bold magenta"),
            ("• ", "white"),
            (f"Uptime: {uptime_fmt}", "dim"),
        )
        layout["header"].update(Panel(Align.center(header_text), style="blue"))
        layout["topology"].update(self._build_rich_topology_panel())
        layout["telemetry"].update(Panel(self._build_rich_telemetry_table(), border_style="cyan"))
        layout["tasks"].update(Panel(self._build_rich_task_state_table(), border_style="green"))

        layout["footer"].split_row(
            Layout(name="stats", ratio=1),
            Layout(name="events", ratio=2),
        )

        stats = self.monitor.stats
        in_flight = self.reliability.in_flight_count if self.reliability else 0
        dlq_count = self.reliability.dead_letter_count if self.reliability else 0
        stats_content = (
            f"[bold]Mesh Metrics & Routing (Week 3/4):[/bold]\n"
            f"• In-Flight Leases: [cyan]{in_flight}[/cyan] | Dead-Letter (DLQ): [red]{dlq_count}[/red]\n"
            f"• Gossip Rounds: [cyan]{stats['gossip_rounds']}[/cyan] | Messages: [green]{stats['messages_sent']} sent[/green]\n"
            f"• Dropouts: [red]{stats['dropouts_detected']}[/red] | Recoveries: [green]{stats['recoveries_detected']}[/green]"
        )
        layout["footer"]["stats"].update(Panel(stats_content, title="Telemetry & Reliability", border_style="magenta"))

        events = self.event_logger.get_recent()
        event_lines = []
        for ts, lvl, msg in events[-4:]:
            color = {"INFO": "green", "GOSSIP": "cyan", "WARN": "yellow", "ALERT": "red", "ROUTING": "magenta"}.get(lvl, "white")
            event_lines.append(f"[dim]{ts}[/dim] [{color}][{lvl}][/{color}] {msg}")
        if not event_lines:
            event_lines.append("[dim]No cluster events logged yet.[/dim]")

        layout["footer"]["events"].update(Panel("\n".join(event_lines), title="Cluster Event Log", border_style="yellow"))
        return layout

    # -------------------------------------------------------------------------
    # Zero-Dependency ANSI/Unicode Terminal Renderer
    # -------------------------------------------------------------------------

    def _render_fallback_snapshot(self) -> str:
        lines = []
        c_cyan = "\033[96m"
        c_green = "\033[92m"
        c_yellow = "\033[93m"
        c_red = "\033[91m"
        c_magenta = "\033[95m"
        c_bold = "\033[1m"
        c_dim = "\033[2m"
        c_reset = "\033[0m"

        w = 104
        lines.append(f"{c_cyan}╔{'═' * (w - 2)}╗{c_reset}")
        title = " MeshWeaver P2P Task Broker — Decentralized Mesh Monitor "
        lines.append(f"{c_cyan}║{c_bold}{title.center(w - 2)}{c_reset}{c_cyan}║{c_reset}")
        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        local_metrics = self.monitor.metrics_collector.collect()
        uptime_fmt = str(datetime.timedelta(seconds=int(local_metrics.uptime_seconds)))
        meta = (
            f" Node: {self.monitor.local_node_id} ({self.monitor.host}:{self.monitor.port}) "
            f"| Mesh: {self.monitor.healthy_peer_count + 1}/{self.monitor.peer_count + 1} Healthy "
            f"| Security: 🔒 TLS + HMAC-SHA256 "
            f"| Uptime: {uptime_fmt} "
        )
        lines.append(f"{c_cyan}║{c_reset}{meta.ljust(w - 2)}{c_cyan}║{c_reset}")
        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        # Topology Section
        lines.append(f"{c_cyan}║{c_reset} {c_bold}[Real-Time Mesh Topology & Inter-Node Links]{c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        peers = sorted(self.monitor.get_all_peers(), key=lambda p: p.node_id)
        topo_line_1 = f" ★ Local Hub: {self.monitor.local_node_id} ({self.monitor.host}:{self.monitor.port}) [ONLINE]"
        lines.append(f"{c_cyan}║{c_reset}{topo_line_1.ljust(w - 2)}{c_cyan}║{c_reset}")

        if not peers:
            lines.append(f"{c_cyan}║{c_reset}   {c_dim}(No peer nodes discovered yet){c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        else:
            for p in peers:
                sec_icon = "🔒 TLS" if p.is_tls else "🔓 PLAIN"
                p_status = f"{c_green}HEALTHY{c_reset}" if p.is_healthy else (f"{c_yellow}SUSPECT{c_reset}" if p.is_suspect else f"{c_red}DEAD{c_reset}")
                rtt = f"{p.round_trip_time_ms:.1f}ms" if p.round_trip_time_ms > 0 else "< 2ms"
                entry = f"   ├── [{sec_icon}] {p.node_id:14} ({p.host}:{p.port}) ──[ Latency: {rtt} | Status: {p_status} ]"
                lines.append(f"{c_cyan}║{c_reset}{entry}".ljust(w + 9) + f"{c_cyan}║{c_reset}")

        lines.append(f"{c_cyan}╠{'═' * 18}╦{'═' * 11}╦{'═' * 11}╦{'═' * 16}╦{'═' * 18}╦{'═' * 18}╦{'═' * 8}╣{c_reset}")

        hdr = (
            f"{c_cyan}║{c_reset} {c_bold}{'Node ID':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Status':9}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Security':9}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Endpoint':14}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'CPU Usage':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'RAM Usage':16}{c_reset} "
            f"{c_cyan}║{c_reset} {c_bold}{'Heartbeat':8}{c_reset}{c_cyan}║{c_reset}"
        )
        lines.append(hdr)
        lines.append(f"{c_cyan}╠{'═' * 18}╬{'═' * 11}╬{'═' * 11}╬{'═' * 16}╬{'═' * 18}╬{'═' * 18}╬{'═' * 8}╣{c_reset}")

        # Local node row
        local_cpu_bar = _render_bar(local_metrics.cpu_percent, width=8)
        local_ram_bar = _render_bar(local_metrics.ram_percent, width=8)
        row_local = (
            f"{c_cyan}║{c_reset} {c_green}★ {self.monitor.local_node_id[:14]:14}{c_reset} "
            f"{c_cyan}║{c_reset} {c_green}{'HEALTHY':9}{c_reset} "
            f"{c_cyan}║{c_reset} {c_magenta}{'🔒 TLS':9}{c_reset} "
            f"{c_cyan}║{c_reset} {f'{self.monitor.host}:{self.monitor.port}':14} "
            f"{c_cyan}║{c_reset} {local_cpu_bar:16} "
            f"{c_cyan}║{c_reset} {local_ram_bar:16} "
            f"{c_cyan}║{c_reset} {'0.0s':8}{c_cyan}║{c_reset}"
        )
        lines.append(row_local)

        # Peer rows
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
            sec_tag = f"{c_magenta}🔒 TLS{c_reset}   " if peer.is_tls else "🔓 PLAIN "

            row_peer = (
                f"{c_cyan}║{c_reset} {peer.node_id[:16]:16} "
                f"{c_cyan}║{c_reset} {status_col} "
                f"{c_cyan}║{c_reset} {sec_tag} "
                f"{c_cyan}║{c_reset} {f'{peer.host}:{peer.port}':14} "
                f"{c_cyan}║{c_reset} {cpu_bar:16} "
                f"{c_cyan}║{c_reset} {ram_bar:16} "
                f"{c_cyan}║{c_reset} {elapsed_str:8}{c_cyan}║{c_reset}"
            )
            lines.append(row_peer)

        # Task Execution State Section
        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")
        lines.append(f"{c_cyan}║{c_reset} {c_bold}[Live Task Execution & Fault Tolerance State]{c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")

        in_flight = self.reliability.in_flight_count if self.reliability else 0
        dlq_count = self.reliability.dead_letter_count if self.reliability else 0
        task_summary = f" Active In-Flight Leases: {in_flight} | Dead-Letter Tasks: {dlq_count} | Routing: LOWEST_CPU"
        lines.append(f"{c_cyan}║{c_reset}{task_summary.ljust(w - 2)}{c_cyan}║{c_reset}")

        if self.reliability:
            leases = self.reliability.get_active_leases()
            if leases:
                lines.append(f"{c_cyan}║{c_reset}   Task ID   | Workload Name        | Assigned Worker | Status   | Retries | Security".ljust(w - 2) + f"{c_cyan}║{c_reset}")
                for lease in leases[:3]:
                    t = lease.task
                    sig_label = "🔒 Signed" if t.signature else "Unsigned"
                    row = f"   {t.task_id[:8]}  | {t.name:20} | → {lease.worker_node_id:13} | RUNNING  | {t.retry_count}/{t.max_retries}     | {sig_label}"
                    lines.append(f"{c_cyan}║{c_reset}{row}".ljust(w - 2) + f"{c_cyan}║{c_reset}")

            dlq = self.reliability.get_dead_letter_tasks()
            if dlq:
                lines.append(f"{c_cyan}║{c_reset}   {c_red}[DEAD-LETTER QUEUE - Permanently Failed Tasks]{c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
                for t, err in dlq[-2:]:
                    row = f"   {c_red}✖ {t.task_id[:8]} ({t.name}): {err[:55]}{c_reset}"
                    lines.append(f"{c_cyan}║{c_reset}{row}".ljust(w + 8) + f"{c_cyan}║{c_reset}")

        lines.append(f"{c_cyan}╠{'═' * (w - 2)}╣{c_reset}")

        # Events Section
        lines.append(f"{c_cyan}║{c_reset} {c_bold}[Cluster Event Log]{c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        events = self.event_logger.get_recent()
        if not events:
            lines.append(f"{c_cyan}║{c_reset} {c_dim}(No cluster events logged yet){c_reset}".ljust(w + 8) + f"{c_cyan}║{c_reset}")
        else:
            for ts, lvl, msg in events[-3:]:
                color = {"INFO": c_green, "GOSSIP": c_cyan, "WARN": c_yellow, "ALERT": c_red, "ROUTING": c_magenta}.get(lvl, "")
                entry = f" {c_dim}{ts}{c_reset} {color}[{lvl}]{c_reset} {msg}"
                lines.append(f"{c_cyan}║{c_reset}{entry}".ljust(w + 14) + f"{c_cyan}║{c_reset}")

        lines.append(f"{c_cyan}╚{'═' * (w - 2)}╝{c_reset}")
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # Public Display Methods
    # -------------------------------------------------------------------------

    def render_snapshot(self) -> str:
        """Returns the current dashboard snapshot as a formatted string."""
        if _HAS_RICH and self._console:
            with self._console.capture() as capture:
                self._console.print(self._build_rich_layout())
            return capture.get()
        return self._render_fallback_snapshot()

    def print_snapshot(self) -> None:
        """Prints the dashboard snapshot to stdout."""
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass
        if _HAS_RICH and self._console:
            self._console.print(self._build_rich_layout())
        else:
            try:
                print(self._render_fallback_snapshot())
            except UnicodeEncodeError:
                out = self._render_fallback_snapshot().encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8")
                print(out)

    async def run_live(self, refresh_seconds: float = 1.0, max_ticks: Optional[int] = None) -> None:
Gossip,-Fault-Tolerance-&-Dashboard-Developer
        """Runs the live terminal dashboard."""

        """Runs the live-updating dashboard in the terminal."""
        if hasattr(sys.stdout, "reconfigure"):
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

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
                try:
                    print(self._render_fallback_snapshot())
                except UnicodeEncodeError:
                    out = self._render_fallback_snapshot().encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8")
                    print(out)
                await asyncio.sleep(refresh_seconds)
                tick += 1
                if max_ticks and tick >= max_ticks:
                    break
