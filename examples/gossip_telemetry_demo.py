"""
Gossip Telemetry, Failure Detection & Dashboard Demo for Member 5.

Shows:
1. Local CPU and memory metrics collection.
2. Background gossip exchange between 5 simulated mesh nodes.
3. Routing a compute task to the lowest-load node.
4. Detecting a node crash when heartbeats stop, and automatically reassigning the task.
5. Displaying the terminal dashboard.
"""

import asyncio
import os
import sys
import time
from typing import Dict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger, RichTerminalDashboard
from gossip.metrics import SystemMetricsCollector
from gossip.models import (
    GossipDigest,
    GossipMessage,
    GossipMessageType,
    NodeMetrics,
    PeerHealthState,
)
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter
from task.models import Task


async def run_member5_demo():
    print("=" * 76)
    print("   MESHWEAVER: GOSSIP PROTOCOL & FAULT TOLERANCE DEMO (MEMBER 5)")
    print("=" * 76)

    # -------------------------------------------------------------------------
    # Step 1: Read local host metrics
    # -------------------------------------------------------------------------
    print("\n[Step 1] Reading Local System Metrics")
    print("-" * 76)

    collector = SystemMetricsCollector(node_id="local_node_01")
    metrics = collector.collect()

    source = "psutil" if collector.has_psutil() else "standard os/proc fallback"
    print(f"• Node ID       : {metrics.node_id}")
    print(f"• Metrics Source: {source}")
    print(f"• CPU Usage     : {metrics.cpu_percent:.1f}%")
    print(f"• RAM Usage     : {metrics.ram_percent:.1f}% ({metrics.memory_used_mb:.1f} MB / {metrics.memory_total_mb:.1f} MB)")
    print(f"• 1-Min Load    : {metrics.load_average_1m}")
    print(f"• Load Score    : {metrics.composite_score:.2f} (60% CPU + 30% Task Pressure + 10% RAM)")
    print("✓ Local metrics sampled successfully.")

    # -------------------------------------------------------------------------
    # Step 2: Spin up a 5-node test cluster
    # -------------------------------------------------------------------------
    print("\n[Step 2] Simulating a 5-Node P2P Mesh")
    print("-" * 76)
    print("Creating 5 nodes with different CPU and RAM loads:")

    node_configs = [
        ("node_gateway", 8000, 15.0, 30.0, 0),
        ("worker_alpha", 8001, 22.0, 40.0, 1),
        ("worker_beta",  8002, 75.0, 85.0, 5),   # Busy worker
        ("worker_gamma", 8003, 10.5, 25.0, 0),   # Idle worker (lowest load)
        ("worker_delta", 8004, 48.0, 55.0, 2),
    ]

    monitors: Dict[str, GossipMonitor] = {}
    event_logger = DashboardEventLogger()

    for nid, port, cpu, ram, tasks in node_configs:
        m = GossipMonitor(
            local_node_id=nid,
            host="127.0.0.1",
            port=port,
            gossip_interval=5.0,
            heartbeat_timeout=10.0,
            fanout=3,
        )
        m.metrics_collector.set_simulated_metrics(cpu_percent=cpu, ram_percent=ram, active_tasks=tasks)
        monitors[nid] = m
        print(f"  • {nid:14} (Port {port}): CPU {cpu:4.1f}%, RAM {ram:4.1f}%, Active Tasks: {tasks}")
        event_logger.log("INFO", f"Node {nid} online on port {port}")

    # Simulated in-memory network transport
    async def make_bus_send(src_id: str):
        async def _send(host: str, port: int, data: bytes):
            msg = GossipMessage.from_bytes(data)
            for target_id, target_monitor in monitors.items():
                if target_id != src_id and target_monitor.port == port:
                    resp = await target_monitor.handle_incoming_message(msg)
                    if resp:
                        await monitors[src_id].handle_incoming_message(resp)
        return _send

    for nid, m in monitors.items():
        m.transport_send_fn = await make_bus_send(nid)

    # Gateway connects to workers; workers connect to gateway
    gateway = monitors["node_gateway"]
    for nid, port, _, _, _ in node_configs[1:]:
        gateway.add_peer(nid, port=port)
        monitors[nid].add_peer("node_gateway", port=8000)

    # -------------------------------------------------------------------------
    # Step 3: Gossip exchange (Week 2 requirement: every 5 seconds)
    # -------------------------------------------------------------------------
    print("\n[Step 3] Exchanging Gossip Telemetry (Every 5s)")
    print("-" * 76)
    print("Broadcasting metrics and exchanging anti-entropy digests...")

    for nid, m in monitors.items():
        sent = await m.broadcast_metrics()
        event_logger.log("GOSSIP", f"Node {nid} sent metrics to {sent} neighbors")

    for nid, m in monitors.items():
        await m.initiate_anti_entropy()

    print(f"✓ Gossip round completed. Gateway tracks {gateway.peer_count} peers:")
    for p in gateway.get_all_peers():
        print(f"  → Peer {p.node_id:14}: CPU={p.metrics.cpu_percent:4.1f}%, RAM={p.metrics.ram_percent:4.1f}%, Status={p.health_state.value}")

    # -------------------------------------------------------------------------
    # Step 4: Workload routing via TaskRouter
    # -------------------------------------------------------------------------
    print("\n[Step 4] Routing Task to Worker with Lowest Load")
    print("-" * 76)

    router = TaskRouter(local_node_id="node_gateway", default_strategy=RoutingStrategy.LOWEST_CPU)
    reliability = ReliabilityManager(default_max_retries=3)

    gateway.sync_with_task_router(router)
    gateway.sync_with_reliability_manager(reliability, router)

    for p in gateway.get_all_peers():
        router.update_worker_metrics(
            node_id=p.node_id,
            cpu_percent=p.metrics.cpu_percent,
            ram_percent=p.metrics.ram_percent,
            active_tasks=p.metrics.active_tasks,
        )

    task = Task(name="image_blur_filter", payload=b"sample_data")
    chosen = router.select_worker(task)
    print(f"• TaskRouter selected worker:")
    print(f"  → Best Candidate : {chosen.node_id} (CPU: {chosen.cpu_percent}%, Load Score: {chosen.composite_load_score:.1f})")
    assert chosen.node_id == "worker_gamma", f"Expected worker_gamma (lowest load), got {chosen.node_id}"

    reliability.assign_task(task, worker_node_id=chosen.node_id)
    print(f"• Assigned task {task.task_id[:8]} to {chosen.node_id}. Active leases: {reliability.in_flight_count}")

    # -------------------------------------------------------------------------
    # Step 5: Simulate worker failure & task recovery
    # -------------------------------------------------------------------------
    print("\n[Step 5] Simulating Node Failure and Re-routing Orphaned Tasks")
    print("-" * 76)
    print("Simulating worker_gamma crash (stops sending heartbeats)...")

    # Simulate 12 seconds passing without heartbeat (> 10s timeout)
    gamma_peer = gateway.get_peer("worker_gamma")
    gamma_peer.last_heartbeat_time = time.time() - 12.0

    newly_suspect, newly_dead = gateway.check_peer_health()
    print(f"• Heartbeat Evaluation:")
    print(f"  → Nodes marked DEAD: {newly_dead}")

    reassigned_lease = reliability._active_leases.get(task.task_id)
    print(f"• Task Recovery:")
    print(f"  → Task {task.task_id[:8]} recovered from dead node: worker_gamma")
    print(f"  → Reassigned to Worker: {reassigned_lease.worker_node_id} (Attempt #{task.retry_count + 1})")
    assert reassigned_lease.worker_node_id != "worker_gamma", "Task should not remain on dead node"

    # -------------------------------------------------------------------------
    # Step 6: Cluster dashboard display
    # -------------------------------------------------------------------------
    print("\n[Step 6] Cluster Dashboard Snapshot")
    print("-" * 76)

    dashboard = RichTerminalDashboard(
        monitor=gateway,
        router=router,
        reliability=reliability,
        event_logger=event_logger,
    )
    dashboard.print_snapshot()

    print("\n" + "=" * 76)
    print("   ALL CHECKS PASSED: WEEKS 1 & 2 FUNCTIONALITY VERIFIED")
    print("=" * 76)


if __name__ == "__main__":
    asyncio.run(run_member5_demo())
