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


class TestMember5Week3Deliverables(unittest.TestCase):
    """
    Validates Member 5 Week 3 responsibilities:
    - Fault Tolerance & Heartbeat Failure Detection
    - Dynamic Gossip Telemetry feeding Lowest-CPU Task Routing
    - Automatic Orphaned Task Recovery & Re-routing upon Node Dropout
    """

    def test_gossip_telemetry_feeds_lowest_cpu_task_router(self):
        """Gossip updates automatically inform TaskRouter for lowest-CPU task selection."""
        monitor = GossipMonitor(local_node_id="gateway")
        from scheduler.router import TaskRouter, RoutingStrategy
        from task.models import Task

        router = TaskRouter(local_node_id="gateway")
        monitor.sync_with_task_router(router)

        # Simulate gossip metrics update: worker_busy (80% CPU), worker_idle (15% CPU)
        m_busy = NodeMetrics(node_id="worker_busy", cpu_percent=80.0)
        m_idle = NodeMetrics(node_id="worker_idle", cpu_percent=15.0)

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(monitor.handle_incoming_message(
                GossipMessage(sender_id="worker_busy", message_type=GossipMessageType.METRICS_UPDATE, payload={"metrics": m_busy.to_dict()})
            ))
            loop.run_until_complete(monitor.handle_incoming_message(
                GossipMessage(sender_id="worker_idle", message_type=GossipMessageType.METRICS_UPDATE, payload={"metrics": m_idle.to_dict()})
            ))
        finally:
            loop.close()

        task = Task(name="compute_workload", payload=b"math")
        selected = router.select_worker(task, strategy=RoutingStrategy.LOWEST_CPU)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "worker_idle")
        self.assertEqual(selected.cpu_percent, 15.0)

    def test_heartbeat_monitor_triggers_automatic_task_rerouting(self):
        """Heartbeat monitor triggers dropout event, automatically re-routing orphaned tasks."""
        from scheduler.router import TaskRouter
        from scheduler.reliability import ReliabilityManager
        from task.models import Task, TaskStatus

        monitor = GossipMonitor(local_node_id="hub", gossip_interval=1.0, heartbeat_timeout=2.0)
        router = TaskRouter(local_node_id="hub")
        reliability = ReliabilityManager()

        monitor.sync_with_task_router(router)
        monitor.sync_with_reliability_manager(reliability, router)

        router.update_worker_metrics("worker_active", cpu_percent=20.0)
        router.update_worker_metrics("worker_backup", cpu_percent=30.0)

        peer = monitor.add_peer("worker_active")
        monitor.add_peer("worker_backup")

        # Assign task to worker_active
        task = Task(name="monte_carlo", payload=b"calc", max_retries=2)
        reliability.assign_task(task, "worker_active")

        # Worker crashes (heartbeat expires)
        peer.last_heartbeat_time = time.time() - 3.0
        monitor.check_peer_health()

        # Task must have been automatically re-routed to worker_backup!
        assignment = reliability._active_leases.get(task.task_id)
        self.assertIsNotNone(assignment)
        self.assertEqual(assignment.worker_node_id, "worker_backup")
        self.assertEqual(task.retry_count, 1)


class TestMember5Week4Deliverables(unittest.TestCase):
    """
    Validates Member 5 Week 4 responsibilities:
    - Real-Time Mesh Topology Visualization in Dashboard
    - Live Task Execution State (Active Leases, Retries, Dead-Letter Queue)
    - Cryptographic Security & TLS Transport Status Integration
    """

    def test_dashboard_real_time_mesh_topology_visualization(self):
        """Dashboard renders real-time mesh topology, link latency, and TLS encryption badges."""
        monitor = GossipMonitor(local_node_id="hub_alpha", host="127.0.0.1", port=9000)
        p1 = monitor.add_peer("node_beta", host="127.0.0.1", port=9001)
        p1.round_trip_time_ms = 2.4
        p1.is_tls = True

        dash = RichTerminalDashboard(monitor=monitor)
        snapshot = dash.render_snapshot()

        self.assertIn("Mesh Topology", snapshot)
        self.assertIn("hub_alpha", snapshot)
        self.assertIn("node_beta", snapshot)
        self.assertIn("2.4ms", snapshot)
        self.assertIn("TLS", snapshot)

    def test_dashboard_live_task_execution_state_and_dlq(self):
        """Dashboard displays in-flight leased tasks and dead-letter queue entries."""
        from scheduler.router import TaskRouter
        from scheduler.reliability import ReliabilityManager
        from task.models import Task

        monitor = GossipMonitor(local_node_id="hub_dash")
        router = TaskRouter(local_node_id="hub_dash")
        reliability = ReliabilityManager()

        # 1. In-flight task
        task1 = Task(name="active_prime_sieve", payload=b"", origin_node_id="hub_dash")
        task1.signature = "sig_hmac_valid"
        reliability.assign_task(task1, "worker_node_01")

        # 2. Dead-letter task
        task2 = Task(name="failed_calc", payload=b"", max_retries=0)
        reliability.assign_task(task2, "worker_node_02")
        reliability.record_failure_and_retry(task2.task_id, "Fatal out of memory", router)

        dash = RichTerminalDashboard(monitor=monitor, router=router, reliability=reliability)
        snapshot = dash.render_snapshot()

        self.assertIn("Live Task Execution", snapshot)
        self.assertIn("active_prime_sieve", snapshot)
        self.assertIn("worker_node_01", snapshot)
        self.assertIn("DEAD-LETTER", snapshot)
        self.assertIn("failed_calc", snapshot)

    def test_tls_certificates_and_cryptographic_signatures(self):
        """Validates TLS self-signed cert generation and HMAC-SHA256 task signing."""
        from security.tls import generate_self_signed_cert
        from security.crypto import sign_task_request, verify_task_request, SignatureVerificationError
        from task.models import Task
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            cert, key = generate_self_signed_cert(
                os.path.join(tmpdir, "test.crt"),
                os.path.join(tmpdir, "test.key"),
                common_name="test_node"
            )
            self.assertTrue(os.path.exists(cert))
            self.assertTrue(os.path.exists(key))

        task = Task(name="secure_workload", payload=b"secret_bytes")
        secret = b"cluster_secret_key_123"
        sign_task_request(task, secret, "signer_node")
        self.assertIsNotNone(task.signature)
        self.assertTrue(verify_task_request(task, secret))

        # Tampering is detected
        task.payload = b"tampered_bytes"
        with self.assertRaises(SignatureVerificationError):
            verify_task_request(task, secret)


if __name__ == "__main__":
    unittest.main()
