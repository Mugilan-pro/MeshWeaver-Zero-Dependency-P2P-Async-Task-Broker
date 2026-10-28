"""
MeshWeaver Web Dashboard Interactive Demo
Spins up a local gateway node with 4 worker peers (Pi workers & Edge GPU),
registers them with GossipMonitor, TaskRouter, and ReliabilityManager,
starts the zero-dependency Web Dashboard on http://127.0.0.1:8080,
and automatically opens the UI in your web browser.

Usage:
    python examples/web_dashboard_demo.py
"""

import asyncio
import os
import random
import sys
import time
import webbrowser

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger
from dashboard.web_server import MeshWeaverWebServer
from gossip.models import PeerHealthState
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import TaskRouter, WorkerNodeInfo


def main():
    print("=" * 72)
    print("  MESHWEAVER: DECENTRALIZED P2P TASK BROKER - WEB DASHBOARD DEMO")
    print("=" * 72)

    event_logger = DashboardEventLogger(max_events=30)
    monitor = GossipMonitor(
        local_node_id="gateway_node",
        host="127.0.0.1",
        port=8000,
        gossip_interval=2.0,
        heartbeat_timeout=12.0,
    )
    router = TaskRouter(local_node_id="gateway_node")
    reliability = ReliabilityManager()

    # Register 4 realistic peer nodes
    worker_specs = [
        ("pi_worker_01", 8001, 24.5, 42.0),
        ("pi_worker_02", 8002, 68.2, 75.5),
        ("pi_worker_03", 8003, 14.0, 28.3),
        ("edge_gpu_01",  8004, 82.5, 88.0),
    ]

    for nid, port, cpu, ram in worker_specs:
        peer = monitor.add_peer(nid, port=port)
        peer.metrics.cpu_percent = cpu
        peer.metrics.ram_percent = ram
        peer.metrics.active_tasks = random.randint(0, 2)
        peer.metrics.completed_tasks = random.randint(5, 20)

        router.register_or_update_worker(WorkerNodeInfo(
            node_id=nid,
            host="127.0.0.1",
            port=port,
            cpu_percent=cpu,
            ram_percent=ram,
            active_tasks=peer.metrics.active_tasks,
            is_alive=True,
        ))

        event_logger.log("INFO", f"Registered mesh peer {nid} (127.0.0.1:{port})")

    event_logger.log("GOSSIP", "Cluster gossip ring synchronized across 5 nodes")

    # Start the zero-dependency Web Server on port 8080
    host = "127.0.0.1"
    port = 8080
    server = MeshWeaverWebServer(
        host=host,
        port=port,
        monitor=monitor,
        router=router,
        reliability=reliability,
        event_logger=event_logger,
    )

    server.start(background=True)

    url = f"http://{host}:{port}"
    print(f"\n[+] MeshWeaver Web Dashboard is live at: {url}")
    print("[+] Press Ctrl+C in this terminal to stop the server.\n")

    # Try opening the browser
    try:
        webbrowser.open(url)
    except Exception:
        pass

    try:
        # Keep background simulation running
        round_count = 0
        while True:
            time.sleep(2.0)
            round_count += 1
            monitor.stats["gossip_rounds"] = round_count

            # Small metric fluctuations to make the dashboard feel alive
            for nid, _, _, _ in worker_specs:
                p = monitor.get_peer(nid)
                if p and p.is_alive:
                    delta_cpu = random.uniform(-4.0, 4.0)
                    delta_ram = random.uniform(-1.5, 1.5)
                    p.metrics.cpu_percent = max(5.0, min(95.0, p.metrics.cpu_percent + delta_cpu))
                    p.metrics.ram_percent = max(10.0, min(90.0, p.metrics.ram_percent + delta_ram))
                    router.update_worker_metrics(
                        node_id=nid,
                        cpu_percent=p.metrics.cpu_percent,
                        ram_percent=p.metrics.ram_percent,
                        active_tasks=p.metrics.active_tasks,
                    )

            if round_count % 10 == 0:
                event_logger.log("GOSSIP", f"Gossip heartbeat round #{round_count} complete (mesh fully connected)")

    except KeyboardInterrupt:
        print("\nStopping MeshWeaver Web Dashboard server...")
        server.stop()
        print("Done. Goodbye!")


if __name__ == "__main__":
    main()
