"""
Interactive live terminal dashboard demo for MeshWeaver.

Spins up a local node with 4 simulated worker peers,
shows metric fluctuations, and simulates a node dropping offline.
"""

import asyncio
import os
import random
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger, RichTerminalDashboard
from gossip.models import PeerHealthState
from gossip.monitor import GossipMonitor


async def run_dashboard_demo():
    print("Starting MeshWeaver Terminal Dashboard Demo...")
    event_logger = DashboardEventLogger(max_events=10)

    monitor = GossipMonitor(
        local_node_id="gateway_node",
        host="127.0.0.1",
        port=8000,
        gossip_interval=2.0,
        heartbeat_timeout=6.0,
    )

    # 4 simulated worker peers with different initial loads
    workers = [
        ("pi_worker_01", 8001, 24.0, 42.0),
        ("pi_worker_02", 8002, 68.0, 75.0),
        ("pi_worker_03", 8003, 12.0, 28.0),
        ("edge_gpu_01",  8004, 88.0, 92.0),
    ]

    for nid, port, cpu, ram in workers:
        p = monitor.add_peer(nid, port=port)
        p.metrics.cpu_percent = cpu
        p.metrics.ram_percent = ram
        p.metrics.active_tasks = random.randint(0, 3)
        event_logger.log("INFO", f"Registered {nid} ({p.host}:{p.port})")

    dashboard = RichTerminalDashboard(monitor=monitor, event_logger=event_logger)

    print("Running 5 dashboard refresh cycles (with simulated load changes)...")
    await monitor.start()

    try:
        for cycle in range(1, 6):
            # Simulate slight metric shifts
            for nid, port, _, _ in workers:
                p = monitor.get_peer(nid)
                if p and p.is_alive:
                    p.metrics.cpu_percent = max(5.0, min(95.0, p.metrics.cpu_percent + random.uniform(-8, 8)))
                    p.metrics.ram_percent = max(10.0, min(90.0, p.metrics.ram_percent + random.uniform(-3, 3)))
                    p.record_heartbeat(sequence=cycle)

            if cycle == 3:
                dead_peer = monitor.get_peer("pi_worker_02")
                if dead_peer:
                    dead_peer.health_state = PeerHealthState.DEAD
                    dead_peer.last_heartbeat_time = time.time() - 15.0
                    event_logger.log("ALERT", "Lost heartbeat from pi_worker_02 — marked DEAD")

            print(f"\n--- Refresh Tick {cycle}/5 ---")
            dashboard.print_snapshot()
            await asyncio.sleep(1.0)
    finally:
        await monitor.stop()

    print("\n✓ Dashboard demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_dashboard_demo())
