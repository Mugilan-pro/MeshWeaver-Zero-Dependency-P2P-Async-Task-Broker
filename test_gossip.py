"""
Unit Tests for MeshWeaver Gossip & Heartbeat Subsystem
Tests telemetry collection, gossip ingestion, heartbeat expiration sweeps,
and dropout recovery triggers.
"""

import time
import unittest

from gossip.telemetry import SystemMetricsCollector, NodeTelemetrySnapshot
from gossip.engine import GossipEngine, PeerHealthStatus
from scheduler.router import TaskRouter
from scheduler.reliability import ReliabilityManager
from task.models import Task


class TestGossipAndHeartbeat(unittest.TestCase):
    def setUp(self):
        self.router = TaskRouter(local_node_id="local_node")
        self.reliability = ReliabilityManager()
        self.gossip = GossipEngine(
            node_id="local_node",
            router=self.router,
            reliability=self.reliability,
            heartbeat_timeout_seconds=1.0,  # fast timeout for testing
        )

    def test_system_metrics_collector(self):
        collector = SystemMetricsCollector(node_id="worker_01")
        collector.set_simulation_override(cpu_percent=45.5, ram_percent=30.0)

        snapshot = collector.capture_snapshot(active_tasks=2, total_completed=10)
        self.assertEqual(snapshot.cpu_percent, 45.5)
        self.assertEqual(snapshot.ram_percent, 30.0)
        self.assertEqual(snapshot.active_tasks, 2)
        self.assertEqual(snapshot.total_completed, 10)

        as_dict = snapshot.to_dict()
        restored = NodeTelemetrySnapshot.from_dict(as_dict)
        self.assertEqual(restored.node_id, "worker_01")
        self.assertEqual(restored.cpu_percent, 45.5)

    def test_incoming_gossip_updates_router_and_heartbeat(self):
        gossip_data = {
            "node_id": "remote_peer_01",
            "host": "127.0.0.1",
            "port": 9001,
            "cpu_percent": 15.0,
            "ram_percent": 25.0,
            "active_tasks": 1,
            "total_completed": 5,
        }

        self.gossip.handle_incoming_gossip(gossip_data)

        # Worker should be registered and healthy in router
        healthy = self.router.get_healthy_workers()
        self.assertEqual(len(healthy), 1)
        self.assertEqual(healthy[0].node_id, "remote_peer_01")
        self.assertEqual(healthy[0].cpu_percent, 15.0)

    def test_heartbeat_timeout_triggers_worker_dropout(self):
        # 1. Register peer with active heartbeat
        gossip_data = {
            "node_id": "peer_dropout_test",
            "host": "127.0.0.1",
            "port": 9002,
            "cpu_percent": 20.0,
        }
        self.gossip.handle_incoming_gossip(gossip_data)

        # 2. Assign in-flight task to this peer
        task = Task(name="important_task", payload=b"")
        self.reliability.assign_task(task, "peer_dropout_test")
        self.assertEqual(self.reliability.in_flight_count, 1)

        # 3. Simulate passage of time beyond heartbeat timeout
        peer = self.gossip._tracked_peers["peer_dropout_test"]
        peer.last_heartbeat = time.time() - 2.0  # 2.0s ago > 1.0s timeout

        # 4. Sweep heartbeats
        dropped = self.gossip.check_heartbeats()
        self.assertIn("peer_dropout_test", dropped)
        self.assertFalse(peer.is_alive)

        # 5. Worker must be marked DEAD in TaskRouter
        healthy = self.router.get_healthy_workers()
        self.assertEqual(len(healthy), 0)

        # 6. Task must be recovered and re-routed by ReliabilityManager
        # Since no other worker is currently registered, task is tracked for retry
        self.assertEqual(task.retry_count, 1)


if __name__ == "__main__":
    unittest.main()
