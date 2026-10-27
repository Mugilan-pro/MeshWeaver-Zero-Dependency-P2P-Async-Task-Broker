"""
MeshWeaver Member 5 Comprehensive Test Suite
Executes all Gossip Protocol, Failure Detection, Heartbeat Health,
and Rich Terminal Dashboard tests for Weeks 1 and 2.
"""

import asyncio
import os
import sys
import time
import unittest

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger, RichTerminalDashboard, _render_bar
from gossip.metrics import SystemMetricsCollector
from gossip.models import (
    GossipDigest,
    GossipMessage,
    GossipMessageType,
    NodeMetrics,
    PeerHealthState,
    PeerNodeRecord,
)
from gossip.monitor import GossipMonitor
from tests.test_gossip import *
from tests.test_health import *


class TestMember5Week1Deliverables(unittest.TestCase):
    """
    Validates Member 5 Week 1 responsibilities:
    - Node CPU and RAM metrics collection
    - Wire-safe heartbeat ping and keepalive messages
    - Payload serialization and format headers
    """

    def test_metrics_collection_baseline(self):
        collector = SystemMetricsCollector(node_id="week1_node")
        metrics = collector.collect()
        self.assertEqual(metrics.node_id, "week1_node")
        self.assertIsInstance(metrics.cpu_percent, float)
        self.assertIsInstance(metrics.ram_percent, float)
        self.assertGreaterEqual(metrics.cpu_percent, 0.0)
        self.assertGreaterEqual(metrics.ram_percent, 0.0)

    def test_heartbeat_ping_wire_serialization(self):
        msg = GossipMessage(
            sender_id="node_ping_sender",
            message_type=GossipMessageType.HEARTBEAT,
            sequence_number=1,
            payload={"host": "127.0.0.1", "port": 8000, "generation": 100},
        )
        wire_data = msg.to_bytes()
        self.assertIsInstance(wire_data, bytes)

        restored = GossipMessage.from_bytes(wire_data)
        self.assertEqual(restored.sender_id, "node_ping_sender")
        self.assertEqual(restored.message_type, GossipMessageType.HEARTBEAT)
        self.assertEqual(restored.sequence_number, 1)
        self.assertEqual(restored.payload["generation"], 100)


class TestMember5Week2Deliverables(unittest.IsolatedAsyncioTestCase):
    """
    Validates Member 5 Week 2 responsibilities:
    - Background task sharing CPU/RAM usage with neighbors every 5 seconds
    - Anti-entropy digest exchange and state convergence
    - Fault tolerance: detecting lost heartbeats and flagging dead peers
    - Rich terminal dashboard visualization
    """

    async def test_gossip_interval_configuration(self):
        monitor = GossipMonitor(
            local_node_id="node_5s_interval",
            gossip_interval=5.0,
            heartbeat_timeout=15.0,
            fanout=3,
        )
        self.assertEqual(monitor.gossip_interval, 5.0)
        self.assertEqual(monitor.heartbeat_timeout, 15.0)
        self.assertEqual(monitor.fanout, 3)

    async def test_periodic_metrics_broadcast_and_receipt(self):
        # Create 2 nodes
        node_1 = GossipMonitor(local_node_id="node_1", gossip_interval=0.2)
        node_1.metrics_collector.set_simulated_metrics(cpu_percent=33.3, ram_percent=44.4)
        node_2 = GossipMonitor(local_node_id="node_2", gossip_interval=0.2)

        # Wire communication
        async def send_to_node_2(host, port, data):
            msg = GossipMessage.from_bytes(data)
            await node_2.handle_incoming_message(msg)

        node_1.transport_send_fn = send_to_node_2
        node_1.add_peer("node_2", port=8002)

        # Broadcast metrics
        sent = await node_1.broadcast_metrics()
        self.assertEqual(sent, 1)

        # Node 2 should now have node 1's metrics
        peer_on_2 = node_2.get_peer("node_1")
        self.assertIsNotNone(peer_on_2)
        self.assertEqual(peer_on_2.metrics.cpu_percent, 33.3)
        self.assertEqual(peer_on_2.metrics.ram_percent, 44.4)

    async def test_dashboard_snapshot_rendering(self):
        monitor = GossipMonitor(local_node_id="dash_test_node")
        p = monitor.add_peer("peer_dash", port=8081)
        p.metrics.cpu_percent = 50.0
        p.metrics.ram_percent = 70.0

        event_logger = DashboardEventLogger()
        event_logger.log("INFO", "Cluster node started")
        event_logger.log("GOSSIP", "Gossip snapshot exchanged")

        dash = RichTerminalDashboard(monitor=monitor, event_logger=event_logger)
        snapshot = dash.render_snapshot()
        self.assertIsInstance(snapshot, str)
        self.assertIn("dash_test_node", snapshot)
        self.assertIn("peer_dash", snapshot)

    def test_progress_bar_formatter(self):
        bar_0 = _render_bar(0.0, width=10)
        self.assertIn("░" * 10, bar_0)
        self.assertIn("0.0%", bar_0)

        bar_100 = _render_bar(100.0, width=10)
        self.assertIn("█" * 10, bar_100)
        self.assertIn("100.0%", bar_100)

        bar_50 = _render_bar(50.0, width=10)
        self.assertIn("█████░░░░░", bar_50)
        self.assertIn("50.0%", bar_50)


if __name__ == "__main__":
    unittest.main()
