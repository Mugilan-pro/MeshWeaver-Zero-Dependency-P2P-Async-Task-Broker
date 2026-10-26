"""
MeshWeaver Zero-Dependency Web Dashboard Server
Serves a lightweight real-time browser interface and REST API
without requiring any third-party pip dependencies.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import random
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger
from gossip.models import PeerHealthState
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import TaskRouter, RoutingStrategy, WorkerNodeInfo
from task.executor import TaskExecutor
from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.serializer import TaskSerializer

logger = logging.getLogger("MeshWeaver.WebServer")


# -----------------------------------------------------------------------------
# Compute Task Benchmark Functions
# -----------------------------------------------------------------------------

def compute_prime_count(limit: int) -> int:
    """Computes prime numbers count up to limit using Sieve of Eratosthenes."""
    if limit < 2:
        return 0
    clamped = min(limit, 2_000_000)
    sieve = [True] * (clamped + 1)
    sieve[0] = sieve[1] = False
    p = 2
    while p * p <= clamped:
        if sieve[p]:
            for i in range(p * p, clamped + 1, p):
                sieve[i] = False
        p += 1
    return sum(1 for is_p in sieve if is_p)


def compute_monte_carlo_pi(samples: int) -> float:
    """Estimates Pi using Monte Carlo random sampling."""
    clamped = min(max(100, samples), 1_000_000)
    inside = 0
    for _ in range(clamped):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return round((4.0 * inside) / clamped, 6)


def compute_matrix_multiply(size: int) -> float:
    """Performs square matrix multiplication of size x size."""
    s = min(max(5, size), 80)
    a = [[random.random() for _ in range(s)] for _ in range(s)]
    b = [[random.random() for _ in range(s)] for _ in range(s)]
    c = [[0.0 for _ in range(s)] for _ in range(s)]
    for i in range(s):
        for j in range(s):
            val = 0.0
            for k in range(s):
                val += a[i][k] * b[k][j]
            c[i][j] = val
    return round(sum(sum(row) for row in c), 4)


def compute_sha256_chain(rounds: int) -> str:
    """Computes iterated SHA-256 hash chain."""
    r = min(max(10, rounds), 50_000)
    h = b"meshweaver_genesis_seed"
    for _ in range(r):
        h = hashlib.sha256(h).digest()
    return h.hex()[:16]


def compute_fibonacci(n: int) -> int:
    """Computes Fibonacci number iteratively."""
    clamped = min(max(1, n), 20_000)
    a, b = 0, 1
    for _ in range(clamped):
        a, b = b, a + b
    return a


TASK_REGISTRY: Dict[str, Tuple[str, Callable[..., Any]]] = {
    "PrimeCount": ("Prime Number Sieve", compute_prime_count),
    "MonteCarloPi": ("Monte Carlo Pi Estimation", compute_monte_carlo_pi),
    "MatrixMultiply": ("Matrix Multiplication", compute_matrix_multiply),
    "SHA256Chain": ("SHA-256 Hash Chain", compute_sha256_chain),
    "Fibonacci": ("Fibonacci Sequence", compute_fibonacci),
}


# -----------------------------------------------------------------------------
# HTTP Request Handler
# -----------------------------------------------------------------------------

class MeshWeaverHTTPRequestHandler(BaseHTTPRequestHandler):
    """Zero-dependency HTTP handler for MeshWeaver Web Dashboard."""

    server_app: MeshWeaverWebServer

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stdout logging for clean console."""
        pass

    def _send_json(self, status_code: int, data: Any) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        path = self.path.split("?")[0]

        if path in ("/", "/index.html"):
            html_content = self.server_app.get_index_html()
            body = html_content.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        elif path == "/api/status":
            self._send_json(200, self.server_app.get_cluster_status())
            return

        elif path == "/api/nodes":
            self._send_json(200, self.server_app.get_nodes_list())
            return

        elif path == "/api/events":
            self._send_json(200, self.server_app.get_events_list())
            return

        self._send_json(404, {"error": "NotFound", "path": path})

    def do_POST(self) -> None:
        path = self.path.split("?")[0]
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len).decode("utf-8") if content_len > 0 else "{}"
        try:
            payload = json.loads(body) if body else {}
        except Exception:
            payload = {}

        if path == "/api/tasks":
            result = self.server_app.handle_dispatch_task(payload)
            self._send_json(200, result)
            return

        elif path == "/api/simulate/crash":
            node_id = payload.get("node_id")
            success = self.server_app.simulate_crash(node_id)
            self._send_json(200, {"success": success, "node_id": node_id})
            return

        elif path == "/api/simulate/recover":
            node_id = payload.get("node_id")
            success = self.server_app.simulate_recover(node_id)
            self._send_json(200, {"success": success, "node_id": node_id})
            return

        elif path == "/api/simulate/fluctuate":
            self.server_app.simulate_fluctuation()
            self._send_json(200, {"success": True})
            return

        self._send_json(404, {"error": "NotFound", "path": path})


# -----------------------------------------------------------------------------
# Main Web Server Application
# -----------------------------------------------------------------------------

class MeshWeaverWebServer:
    """Manages the background HTTP server and binds MeshWeaver subsystems."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        monitor: Optional[GossipMonitor] = None,
        router: Optional[TaskRouter] = None,
        reliability: Optional[ReliabilityManager] = None,
        event_logger: Optional[DashboardEventLogger] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.monitor = monitor or GossipMonitor(local_node_id="gateway_node", host=host, port=8000)
        self.router = router or TaskRouter(local_node_id=self.monitor.local_node_id)
        self.reliability = reliability or ReliabilityManager()
        self.event_logger = event_logger or DashboardEventLogger(max_events=20)
        self.executor = TaskExecutor(node_id=self.monitor.local_node_id)

        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._is_running = False
        self._total_completed_tasks = 0

        # Wire monitor callbacks to event logger and router
        self._wire_subsystem_events()

    def _wire_subsystem_events(self) -> None:
        def on_dropout(node_id: str) -> None:
            self.event_logger.log("ALERT", f"Heartbeat lost: {node_id} marked DEAD")
            self.router.mark_worker_dead(node_id)
            orphaned = self.reliability.handle_worker_dropout(node_id, self.router)
            if orphaned:
                self.event_logger.log("WARN", f"Reassigned {len(orphaned)} orphaned task(s) from {node_id}")

        def on_recovered(node_id: str) -> None:
            self.event_logger.log("INFO", f"Node {node_id} heartbeat restored (HEALTHY)")
            p = self.monitor.get_peer(node_id)
            if p:
                self.router.update_worker_metrics(
                    node_id=node_id,
                    cpu_percent=p.metrics.cpu_percent,
                    ram_percent=p.metrics.ram_percent,
                    active_tasks=p.metrics.active_tasks,
                )

        def on_suspect(node_id: str) -> None:
            self.event_logger.log("WARN", f"Missed heartbeat from {node_id} (SUSPECT)")

        self.monitor.on_peer_dropout = on_dropout
        self.monitor.on_peer_recovered = on_recovered
        self.monitor.on_peer_suspect = on_suspect

    def get_index_html(self) -> str:
        """Reads static index.html or returns fallback."""
        static_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
        if os.path.exists(static_path):
            with open(static_path, "r", encoding="utf-8") as f:
                return f.read()
        return "<html><body><h1>MeshWeaver Web UI</h1><p>index.html not found</p></body></html>"

    def get_cluster_status(self) -> Dict[str, Any]:
        """Returns high-level summary of the mesh cluster."""
        local_metrics = self.monitor.metrics_collector.collect()
        all_nodes = [local_metrics] + [p.metrics for p in self.monitor._peers.values()]
        
        healthy_count = (1 if True else 0) + self.monitor.healthy_peer_count
        total_nodes = 1 + self.monitor.peer_count

        avg_cpu = sum(m.cpu_percent for m in all_nodes) / max(1, len(all_nodes))
        avg_ram = sum(m.ram_percent for m in all_nodes) / max(1, len(all_nodes))

        return {
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_count,
            "completed_tasks": self._total_completed_tasks,
            "in_flight_tasks": self.reliability.in_flight_count,
            "dead_letter_tasks": self.reliability.dead_letter_count,
            "avg_cpu": round(avg_cpu, 1),
            "avg_ram": round(avg_ram, 1),
            "gossip_rounds": self.monitor.stats.get("gossip_rounds", 0),
            "local_node_id": self.monitor.local_node_id,
            "uptime_seconds": round(local_metrics.uptime_seconds, 1),
        }

    def get_nodes_list(self) -> List[Dict[str, Any]]:
        """Returns detailed telemetry list of all nodes."""
        local_metrics = self.monitor.metrics_collector.collect()
        nodes = [{
            "node_id": self.monitor.local_node_id,
            "host": self.monitor.host,
            "port": self.monitor.port,
            "status": "HEALTHY",
            "cpu_percent": local_metrics.cpu_percent,
            "ram_percent": local_metrics.ram_percent,
            "active_tasks": local_metrics.active_tasks,
            "completed_tasks": local_metrics.completed_tasks,
            "is_local": True,
            "last_heartbeat": "0.0s self",
        }]

        now = time.time()
        for p in self.monitor._peers.values():
            delta = now - p.last_heartbeat_time if p.last_heartbeat_time > 0 else 0
            nodes.append({
                "node_id": p.node_id,
                "host": p.host,
                "port": p.port,
                "status": p.health_state.value,
                "cpu_percent": p.metrics.cpu_percent,
                "ram_percent": p.metrics.ram_percent,
                "active_tasks": p.metrics.active_tasks,
                "completed_tasks": p.metrics.completed_tasks,
                "is_local": False,
                "last_heartbeat": f"{delta:.1f}s ago" if p.is_alive else "DEAD",
            })

        return nodes

    def get_events_list(self) -> List[Dict[str, str]]:
        """Returns formatted recent event logs."""
        return [
            {"timestamp": ts, "level": lvl, "message": msg}
            for ts, lvl, msg in self.event_logger.get_recent()
        ]

    def handle_dispatch_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatches a compute workload, routes to optimal peer, and returns result."""
        preset = payload.get("preset", "PrimeCount")
        strategy_str = payload.get("strategy", "LOWEST_CPU")
        param_val = int(payload.get("parameter", 100000))
        priority_str = payload.get("priority", "NORMAL")

        task_desc, func = TASK_REGISTRY.get(preset, ("Generic Compute", compute_prime_count))

        try:
            strategy = RoutingStrategy(strategy_str)
        except Exception:
            strategy = RoutingStrategy.LOWEST_CPU

        try:
            priority = TaskPriority(priority_str)
        except Exception:
            priority = TaskPriority.NORMAL

        task = TaskSerializer.package_task(
            name=preset,
            func=func,
            args=(param_val,),
            priority=priority,
            timeout_seconds=10.0,
            origin_node_id=self.monitor.local_node_id,
        )

        # Route task
        selected_worker = self.router.select_worker(task, strategy=strategy)
        worker_id = selected_worker.node_id if selected_worker else self.monitor.local_node_id

        # Track lease
        self.reliability.assign_task(task, worker_id)
        self.event_logger.log("TASK", f"Dispatched {preset} to {worker_id} (strategy: {strategy.value})")

        start_time = time.perf_counter()
        # Execute compute callable
        try:
            result_val = func(param_val)
            exec_duration_ms = (time.perf_counter() - start_time) * 1000.0
            status_str = "COMPLETED"

            task_res = TaskResult.success(
                task_id=task.task_id,
                return_value=result_val,
                execution_time_seconds=exec_duration_ms / 1000.0,
                executed_by_node_id=worker_id,
            )
            self.reliability.record_success(task.task_id, task_res)
            self._total_completed_tasks += 1

            # Update worker completed count
            if worker_id != self.monitor.local_node_id:
                peer = self.monitor.get_peer(worker_id)
                if peer:
                    peer.metrics.completed_tasks += 1
            else:
                self.monitor.metrics_collector.collect().completed_tasks += 1

        except Exception as e:
            exec_duration_ms = (time.perf_counter() - start_time) * 1000.0
            result_val = f"Error: {e}"
            status_str = "FAILED"
            self.event_logger.log("ALERT", f"Task {task.task_id[:8]} failed on {worker_id}: {e}")

        return {
            "task_id": task.task_id,
            "task_name": preset,
            "worker_node_id": worker_id,
            "execution_time_ms": round(exec_duration_ms, 2),
            "status": status_str,
            "result": result_val,
            "strategy": strategy.value,
        }

    def simulate_crash(self, node_id: Optional[str]) -> bool:
        """Simulates dropping a peer offline."""
        if not node_id:
            return False
        peer = self.monitor.get_peer(node_id)
        if not peer:
            return False
        peer.health_state = PeerHealthState.DEAD
        self.router.mark_worker_dead(node_id)
        self.event_logger.log("ALERT", f"Simulation: Dropped {node_id} offline (DEAD)")
        self.reliability.handle_worker_dropout(node_id, self.router)
        return True

    def simulate_recover(self, node_id: Optional[str]) -> bool:
        """Simulates recovering a peer back to healthy."""
        if not node_id:
            return False
        peer = self.monitor.get_peer(node_id)
        if not peer:
            return False
        peer.health_state = PeerHealthState.HEALTHY
        peer.last_heartbeat_time = time.time()
        self.router.update_worker_metrics(
            node_id=node_id,
            cpu_percent=peer.metrics.cpu_percent,
            ram_percent=peer.metrics.ram_percent,
            active_tasks=peer.metrics.active_tasks,
        )
        self.event_logger.log("INFO", f"Simulation: Revived {node_id} back to HEALTHY")
        return True

    def simulate_fluctuation(self) -> None:
        """Simulates random metric shifts across live peers."""
        for p in self.monitor._peers.values():
            if p.is_alive:
                delta_cpu = random.uniform(-10.0, 10.0)
                delta_ram = random.uniform(-4.0, 4.0)
                p.metrics.cpu_percent = max(4.0, min(96.0, p.metrics.cpu_percent + delta_cpu))
                p.metrics.ram_percent = max(10.0, min(92.0, p.metrics.ram_percent + delta_ram))
                self.router.update_worker_metrics(
                    node_id=p.node_id,
                    cpu_percent=p.metrics.cpu_percent,
                    ram_percent=p.metrics.ram_percent,
                    active_tasks=p.metrics.active_tasks,
                )
        self.event_logger.log("GOSSIP", "Simulated mesh resource load fluctuation")

    def start(self, background: bool = True) -> None:
        """Starts the HTTP server."""
        if self._is_running:
            return

        handler = MeshWeaverHTTPRequestHandler
        handler.server_app = self  # type: ignore

        self._server = HTTPServer((self.host, self.port), handler)
        self._is_running = True
        logger.info(f"MeshWeaver Web Dashboard running at http://{self.host}:{self.port}")

        if background:
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
        else:
            self._server.serve_forever()

    def stop(self) -> None:
        """Stops the HTTP server."""
        if self._server and self._is_running:
            self._server.shutdown()
            self._server.server_close()
            self._is_running = False
            logger.info("MeshWeaver Web Dashboard server stopped.")


if __name__ == "__main__":
    print(f"Starting MeshWeaver Web UI server on http://127.0.0.1:8080 ...")
    server = MeshWeaverWebServer(host="127.0.0.1", port=8080)
    server.start(background=False)
