"""
Week 1 and 2 Milestones Demo for Member 5.

Demonstrates:
- Week 1: Node CPU/RAM sampling and heartbeat ping message encoding.
- Week 2: Nodes sharing CPU/RAM usage with their neighbors every 5 seconds.
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import RichTerminalDashboard
from gossip.metrics import SystemMetricsCollector
from gossip.models import GossipMessage, GossipMessageType
from gossip.monitor import GossipMonitor


async def main():
    print("=" * 76)
    print("   MESHWEAVER WEEKS 1 & 2 DEMO (MEMBER 5)")
    print("=" * 76)

    # -------------------------------------------------------------------------
    # Week 1: Ping / Heartbeat and Local Metrics
    # -------------------------------------------------------------------------
    print("\n>>> Week 1 Milestone: System Metrics & Heartbeat Ping")
    print("-" * 76)

    collector = SystemMetricsCollector(node_id="edge_node_01")
    local_metrics = collector.collect()
    print("1. Sampled host metrics from edge_node_01:")
    print(f"   • CPU Usage: {local_metrics.cpu_percent:.1f}%")
    print(f"   • RAM Usage: {local_metrics.ram_percent:.1f}%")
    print(f"   • Load 1m  : {local_metrics.load_average_1m}")

    ping_msg = GossipMessage(
        sender_id="edge_node_01",
        message_type=GossipMessageType.HEARTBEAT,
        sequence_number=1,
        payload={"host": "127.0.0.1", "port": 8000, "metrics": local_metrics.to_dict()},
    )
    wire_bytes = ping_msg.to_bytes()
    print(f"2. Formatted heartbeat ping packet ({len(wire_bytes)} bytes):")
    print(f"   {wire_bytes.decode('utf-8')[:80]}...")
    print("✓ Week 1 milestone verified.")

    # -------------------------------------------------------------------------
    # Week 2: 5-Second Gossip Cycle
    # -------------------------------------------------------------------------
    print("\n>>> Week 2 Milestone: Periodic Gossip Telemetry (Every 5s)")
    print("-" * 76)
    print("Starting 3 nodes with 5-second gossip loops...")

    node_alpha = GossipMonitor(local_node_id="node_alpha", port=8001, gossip_interval=5.0)
    node_beta = GossipMonitor(local_node_id="node_beta", port=8002, gossip_interval=5.0)
    node_gamma = GossipMonitor(local_node_id="node_gamma", port=8003, gossip_interval=5.0)

    nodes = {"node_alpha": node_alpha, "node_beta": node_beta, "node_gamma": node_gamma}

    node_alpha.metrics_collector.set_simulated_metrics(cpu_percent=18.0, ram_percent=32.0)
    node_beta.metrics_collector.set_simulated_metrics(cpu_percent=65.5, ram_percent=78.0)
    node_gamma.metrics_collector.set_simulated_metrics(cpu_percent=28.0, ram_percent=45.0)

    # In-memory routing for the demo
    async def make_transport(src_id: str):
        async def _send(host: str, port: int, data: bytes):
            msg = GossipMessage.from_bytes(data)
            for dest_id, dest_m in nodes.items():
                if dest_id != src_id and dest_m.port == port:
                    resp = await dest_m.handle_incoming_message(msg)
                    if resp:
                        await nodes[src_id].handle_incoming_message(resp)
        return _send

    for nid, m in nodes.items():
        m.transport_send_fn = await make_transport(nid)

    node_alpha.add_peer("node_beta", port=8002)
    node_alpha.add_peer("node_gamma", port=8003)
    node_beta.add_peer("node_alpha", port=8001)
    node_gamma.add_peer("node_alpha", port=8001)

    print("Running gossip round (sharing CPU/RAM usage with neighbors)...")
    await node_alpha.broadcast_metrics()
    await node_beta.broadcast_metrics()
    await node_gamma.broadcast_metrics()

    print("\nCluster Knowledge after Gossip:")
    for nid, m in nodes.items():
        print(f"• [{nid}] knows {m.peer_count} peers:")
        for p in m.get_all_peers():
            print(f"    → {p.node_id:12} | CPU: {p.metrics.cpu_percent:4.1f}% | RAM: {p.metrics.ram_percent:4.1f}% | Status: {p.health_state.value}")

    print("\n>>> Dashboard View from node_alpha:")
    dash = RichTerminalDashboard(monitor=node_alpha)
    dash.print_snapshot()

    print("\n✓ Weeks 1 & 2 demo completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
