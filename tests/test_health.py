"""
Unit & Integration Tests for MeshWeaver Heartbeat Health & Fault Tolerance
Validates failure detection state machine (HEALTHY -> SUSPECT -> DEAD -> RECOVERED),
timeout enforcement, dropout alerts, and Task Engine reassignment integration.
"""

import asyncio
import os
import sys
import time
import unittest

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from gossip.models import (
    GossipMessage,
    GossipMessageType,
    NodeMetrics,
    PeerHealthState,
)
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter
from task.models import Task, TaskPriority


class TestHeartbeatHealth(unittest.TestCase):
    """Tests two-tier heartbeat failure detection and recovery state transitions."""

    def setUp(self):
        self.monitor = GossipMonitor(
            local_node_id="local_tester",
            gossip_interval=1.0,
            heartbeat_timeout=2.0,  # 2.0s to declare DEAD, suspect at 1.0s
        )

    def test_initial_peer_healthy(self):
        peer = self.monitor.add_peer("worker_01")
        self.assertEqual(peer.health_state, PeerHealthState.HEALTHY)
        self.assertTrue(peer.is_healthy)
        self.assertFalse(peer.is_suspect)
        self.assertFalse(peer.is_dead)

    def test_transition_to_suspect(self):
        peer = self.monitor.add_peer("worker_01")
        suspect_notifications = []
        self.monitor.on_peer_suspect = lambda nid: suspect_notifications.append(nid)

        # Simulate 1.2s without heartbeat (exceeds suspect_timeout of 1.0s, but < 2.0s)
        peer.last_heartbeat_time = time.time() - 1.2

        newly_suspect, newly_dead = self.monitor.check_peer_health()
        self.assertEqual(newly_suspect, ["worker_01"])
        self.assertEqual(newly_dead, [])
        self.assertEqual(peer.health_state, PeerHealthState.SUSPECT)
        self.assertTrue(peer.is_suspect)
        self.assertEqual(suspect_notifications, ["worker_01"])

    def test_transition_to_dead(self):
        peer = self.monitor.add_peer("worker_01")
        dropout_notifications = []
        self.monitor.on_peer_dropout = lambda nid: dropout_notifications.append(nid)

        # Simulate 2.5s without heartbeat (exceeds heartbeat_timeout of 2.0s)
        peer.last_heartbeat_time = time.time() - 2.5

        newly_suspect, newly_dead = self.monitor.check_peer_health()
        self.assertEqual(newly_dead, ["worker_01"])
        self.assertEqual(peer.health_state, PeerHealthState.DEAD)
        self.assertTrue(peer.is_dead)
        self.assertFalse(peer.is_alive)
        self.assertEqual(dropout_notifications, ["worker_01"])
        self.assertEqual(self.monitor.stats["dropouts_detected"], 1)

    def test_recovery_from_dead_via_heartbeat(self):
        loop = asyncio.new_event_loop()
        try:
            peer = self.monitor.add_peer("worker_01")
            peer.health_state = PeerHealthState.DEAD
            peer.last_heartbeat_time = time.time() - 10.0

            recovered_notifications = []
            self.monitor.on_peer_recovered = lambda nid: recovered_notifications.append(nid)

            # Node sends fresh heartbeat
            hb_msg = GossipMessage(
                sender_id="worker_01",
                message_type=GossipMessageType.HEARTBEAT,
                sequence_number=1,
            )
            loop.run_until_complete(self.monitor.handle_incoming_message(hb_msg))

            self.assertEqual(peer.health_state, PeerHealthState.HEALTHY)
            self.assertTrue(peer.is_healthy)
            self.assertEqual(recovered_notifications, ["worker_01"])
            self.assertEqual(self.monitor.stats["recoveries_detected"], 1)
        finally:
            loop.close()

    def test_recovery_from_dead_via_metrics_update(self):
        loop = asyncio.new_event_loop()
        try:
            peer = self.monitor.add_peer("worker_02")
            peer.health_state = PeerHealthState.DEAD
            peer.last_heartbeat_time = time.time() - 10.0

            metrics = NodeMetrics(node_id="worker_02", cpu_percent=33.0)
            msg = GossipMessage(
                sender_id="worker_02",
                message_type=GossipMessageType.METRICS_UPDATE,
                payload={"metrics": metrics.to_dict()},
            )
            loop.run_until_complete(self.monitor.handle_incoming_message(msg))

            self.assertEqual(peer.health_state, PeerHealthState.HEALTHY)
            self.assertEqual(peer.metrics.cpu_percent, 33.0)
        finally:
            loop.close()


class TestFaultToleranceIntegration(unittest.TestCase):
    """
    Verifies end-to-end integration between Member 5's Gossip/Heartbeat monitor
    and Member 4's TaskRouter and ReliabilityManager.
    """

    def setUp(self):
        self.monitor = GossipMonitor(
            local_node_id="router_node",
            gossip_interval=1.0,
            heartbeat_timeout=2.0,
        )
        self.router = TaskRouter(
            local_node_id="router_node",
            default_strategy=RoutingStrategy.LOWEST_CPU,
        )
        self.reliability = ReliabilityManager(default_max_retries=3)

        # Wire GossipMonitor with TaskRouter & ReliabilityManager
        self.monitor.sync_with_task_router(self.router)
        self.monitor.sync_with_reliability_manager(self.reliability, self.router)

    def test_gossip_metrics_updates_task_router(self):
        loop = asyncio.new_event_loop()
        try:
            # Receive gossip metrics from peer_alpha (CPU 15%) and peer_beta (CPU 80%)
            msg_a = GossipMessage(
                sender_id="peer_alpha",
                message_type=GossipMessageType.METRICS_UPDATE,
                payload={"metrics": {"node_id": "peer_alpha", "cpu_percent": 15.0, "ram_percent": 30.0}},
            )
            msg_b = GossipMessage(
                sender_id="peer_beta",
                message_type=GossipMessageType.METRICS_UPDATE,
                payload={"metrics": {"node_id": "peer_beta", "cpu_percent": 80.0, "ram_percent": 60.0}},
            )
            loop.run_until_complete(self.monitor.handle_incoming_message(msg_a))
            loop.run_until_complete(self.monitor.handle_incoming_message(msg_b))

            # TaskRouter should now know about peer_alpha and peer_beta
            task = Task(name="test_compute", payload=b"payload")
            chosen_worker = self.router.select_worker(task)
            self.assertIsNotNone(chosen_worker)
            self.assertEqual(chosen_worker.node_id, "peer_alpha")
        finally:
            loop.close()

    def test_dropout_triggers_automatic_task_reassignment(self):
        loop = asyncio.new_event_loop()
        try:
            # 1. Register 2 workers via gossip
            msg_worker_1 = GossipMessage(
                sender_id="worker_active",
                message_type=GossipMessageType.METRICS_UPDATE,
                payload={"metrics": {"node_id": "worker_active", "cpu_percent": 20.0}},
            )
            msg_worker_2 = GossipMessage(
                sender_id="worker_backup",
                message_type=GossipMessageType.METRICS_UPDATE,
                payload={"metrics": {"node_id": "worker_backup", "cpu_percent": 40.0}},
            )
            loop.run_until_complete(self.monitor.handle_incoming_message(msg_worker_1))
            loop.run_until_complete(self.monitor.handle_incoming_message(msg_worker_2))

            # 2. Assign in-flight task to worker_active
            task = Task(name="heavy_math", payload=b"math", max_retries=2)
            self.reliability.assign_task(task, worker_node_id="worker_active")
            self.assertEqual(self.reliability.in_flight_count, 1)

            # 3. Simulate worker_active crashing (missing heartbeat)
            peer_1 = self.monitor.get_peer("worker_active")
            peer_1.last_heartbeat_time = time.time() - 3.0

            # 4. Heartbeat check triggers dropout
            newly_suspect, newly_dead = self.monitor.check_peer_health()
            self.assertIn("worker_active", newly_dead)

            # 5. TaskRouter should mark worker_active as dead
            worker_info = self.router._workers.get("worker_active")
            self.assertFalse(worker_info.is_alive)

            # 6. ReliabilityManager should have captured and reassigned task to worker_backup!
            reassigned_assignment = self.reliability._active_leases.get(task.task_id)
            self.assertIsNotNone(reassigned_assignment)
            self.assertEqual(reassigned_assignment.worker_node_id, "worker_backup")
            self.assertEqual(task.retry_count, 1)
        finally:
            loop.close()


if __name__ == "__main__":
    unittest.main()
