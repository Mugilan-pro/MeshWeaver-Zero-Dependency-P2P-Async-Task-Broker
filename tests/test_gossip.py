"""
Unit & Integration Tests for MeshWeaver Gossip Protocol & Metrics Collection
Tests SystemMetricsCollector, NodeMetrics, GossipMessage, anti-entropy digests,
and peer state reconciliation.
"""

import asyncio
import os
import sys
import time
import unittest

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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


class TestMetricsCollector(unittest.TestCase):
    """Verifies CPU/RAM hardware sampling and simulation hooks."""

    def setUp(self):
        self.collector = SystemMetricsCollector(node_id="test_node_01")

    def test_live_metrics_collection(self):
        metrics = self.collector.collect()
        self.assertEqual(metrics.node_id, "test_node_01")
        self.assertGreaterEqual(metrics.cpu_percent, 0.0)
        self.assertLessEqual(metrics.cpu_percent, 100.0)
        self.assertGreaterEqual(metrics.ram_percent, 0.0)
        self.assertLessEqual(metrics.ram_percent, 100.0)
        self.assertGreaterEqual(metrics.memory_total_mb, 0.0)
        self.assertGreater(metrics.timestamp, 0.0)
        self.assertEqual(metrics.sequence_number, 1)

    def test_simulated_metrics_injection(self):
        self.collector.set_simulated_metrics(cpu_percent=42.5, ram_percent=68.0, active_tasks=3)
        metrics = self.collector.collect()
        self.assertEqual(metrics.cpu_percent, 42.5)
        self.assertEqual(metrics.ram_percent, 68.0)
        self.assertEqual(metrics.active_tasks, 3)

        # Clear simulated values
        self.collector.clear_simulated_metrics()
        self.assertIsNone(self.collector._simulated_cpu)
        self.assertIsNone(self.collector._simulated_ram)

    def test_task_counter_provider(self):
        provider = lambda: (5, 20, 2)
        collector = SystemMetricsCollector(node_id="worker_02", task_counter_provider=provider)
        metrics = collector.collect()
        self.assertEqual(metrics.active_tasks, 5)
        self.assertEqual(metrics.completed_tasks, 20)
        self.assertEqual(metrics.failed_tasks, 2)


class TestGossipModels(unittest.TestCase):
    """Tests wire serialization and model transformations."""

    def test_node_metrics_serialization(self):
        original = NodeMetrics(
            node_id="node_a",
            cpu_percent=35.0,
            ram_percent=55.0,
            active_tasks=2,
            uptime_seconds=120.0,
        )
        data = original.to_dict()
        reconstructed = NodeMetrics.from_dict(data)

        self.assertEqual(reconstructed.node_id, "node_a")
        self.assertEqual(reconstructed.cpu_percent, 35.0)
        self.assertEqual(reconstructed.ram_percent, 55.0)
        self.assertEqual(reconstructed.active_tasks, 2)

        # Test composite score calculation
        # Formula: 0.6 * cpu + 0.3 * (active_tasks * 10) + 0.1 * ram
        expected_score = (35.0 * 0.6) + (20.0 * 0.3) + (55.0 * 0.1)
        self.assertAlmostEqual(reconstructed.composite_score, expected_score, places=2)

    def test_peer_node_record(self):
        peer = PeerNodeRecord(node_id="peer_1", host="192.168.1.10", port=8001)
        self.assertTrue(peer.is_alive)
        self.assertTrue(peer.is_healthy)
        self.assertFalse(peer.is_dead)

        # Record heartbeat
        m = NodeMetrics(node_id="peer_1", cpu_percent=12.0)
        peer.record_heartbeat(sequence=10, generation=1, metrics=m, rtt_ms=4.5)
        self.assertEqual(peer.heartbeat_sequence, 10)
        self.assertEqual(peer.generation, 1)
        self.assertEqual(peer.metrics.cpu_percent, 12.0)
        self.assertEqual(peer.round_trip_time_ms, 4.5)

        # Serialize & deserialize
        data = peer.to_dict()
        reconstructed = PeerNodeRecord.from_dict(data)
        self.assertEqual(reconstructed.node_id, "peer_1")
        self.assertEqual(reconstructed.metrics.cpu_percent, 12.0)

    def test_gossip_message_json_and_bytes(self):
        msg = GossipMessage(
            sender_id="node_sender",
            message_type=GossipMessageType.METRICS_UPDATE,
            sequence_number=42,
            payload={"cpu": 25.5, "host": "127.0.0.1"},
        )
        raw_bytes = msg.to_bytes()
        self.assertIsInstance(raw_bytes, bytes)

        parsed = GossipMessage.from_bytes(raw_bytes)
        self.assertEqual(parsed.sender_id, "node_sender")
        self.assertEqual(parsed.message_type, GossipMessageType.METRICS_UPDATE)
        self.assertEqual(parsed.sequence_number, 42)
        self.assertEqual(parsed.payload["cpu"], 25.5)


class TestGossipMonitor(unittest.IsolatedAsyncioTestCase):
    """Tests GossipMonitor peer discovery, message routing, and anti-entropy."""

    async def asyncSetUp(self):
        self.monitor = GossipMonitor(
            local_node_id="node_local",
            host="127.0.0.1",
            port=9000,
            gossip_interval=1.0,
            heartbeat_timeout=3.0,
        )

    async def asyncTearDown(self):
        if self.monitor.is_running:
            await self.monitor.stop()

    def test_add_and_query_peers(self):
        self.monitor.add_peer("worker_1", host="127.0.0.1", port=9001)
        self.monitor.add_peer("worker_2", host="127.0.0.1", port=9002)

        self.assertEqual(self.monitor.peer_count, 2)
        self.assertEqual(self.monitor.live_peer_count, 2)
        self.assertIsNotNone(self.monitor.get_peer("worker_1"))
        self.assertIsNotNone(self.monitor.get_peer("worker_2"))

        # Cannot add local node as peer
        res = self.monitor.add_peer("node_local")
        self.assertIsNone(res)
        self.assertEqual(self.monitor.peer_count, 2)

    async def test_handle_incoming_metrics_update(self):
        self.monitor.add_peer("worker_1")
        metrics_dict = {
            "node_id": "worker_1",
            "cpu_percent": 18.5,
            "ram_percent": 35.0,
            "active_tasks": 2,
        }
        msg = GossipMessage(
            sender_id="worker_1",
            message_type=GossipMessageType.METRICS_UPDATE,
            sequence_number=5,
            payload={"metrics": metrics_dict, "generation": 1},
        )

        callback_received = []
        self.monitor.on_metrics_updated = lambda nid, m: callback_received.append((nid, m.cpu_percent))

        await self.monitor.handle_incoming_message(msg)

        peer = self.monitor.get_peer("worker_1")
        self.assertEqual(peer.metrics.cpu_percent, 18.5)
        self.assertEqual(peer.metrics.ram_percent, 35.0)
        self.assertEqual(len(callback_received), 1)
        self.assertEqual(callback_received[0], ("worker_1", 18.5))

    async def test_anti_entropy_digest_and_delta(self):
        # Setup node A with peer B
        node_a = GossipMonitor(local_node_id="node_a", gossip_interval=1.0)
        node_a.metrics_collector.set_simulated_metrics(cpu_percent=15.0)
        node_a.add_peer("node_b")

        # Setup node B
        node_b = GossipMonitor(local_node_id="node_b", gossip_interval=1.0)
        node_b.metrics_collector.set_simulated_metrics(cpu_percent=85.0)

        # Node B creates digest
        digest_b = node_b.create_state_digest()
        self.assertIn("node_b", digest_b.entries)

        # Node A processes B's digest and computes delta
        delta_for_b = node_a.compute_anti_entropy_delta(digest_b)
        self.assertIn("node_a", delta_for_b)

        # Node B applies delta from node A
        updated = node_b.apply_anti_entropy_delta(delta_for_b)
        self.assertGreaterEqual(updated, 1)

        # Node B should now know about node A with 15.0% CPU
        peer_a_on_b = node_b.get_peer("node_a")
        self.assertIsNotNone(peer_a_on_b)
        self.assertEqual(peer_a_on_b.metrics.cpu_percent, 15.0)

    async def test_simulated_multi_node_gossip_convergence(self):
        """
        Simulates an in-memory mesh of 3 nodes exchanging gossip messages
        and verifies all nodes learn about each other's metrics.
        """
        nodes: Dict[str, GossipMonitor] = {}
        for i in range(3):
            nid = f"mesh_node_{i}"
            m = GossipMonitor(local_node_id=nid, gossip_interval=0.5)
            m.metrics_collector.set_simulated_metrics(cpu_percent=10.0 * (i + 1), ram_percent=20.0 * (i + 1))
            nodes[nid] = m

        # Setup transport: direct in-memory message routing
        async def make_send(src_id: str):
            async def _send(host: str, port: int, data: bytes):
                msg = GossipMessage.from_bytes(data)
                # Dispatch to recipient
                for target_id, target_monitor in nodes.items():
                    if target_id != src_id:
                        resp = await target_monitor.handle_incoming_message(msg)
                        if resp:
                            await nodes[src_id].handle_incoming_message(resp)
            return _send

        for nid, m in nodes.items():
            m.transport_send_fn = await make_send(nid)

        # Connect in a ring: 0 -> 1 -> 2 -> 0
        nodes["mesh_node_0"].add_peer("mesh_node_1")
        nodes["mesh_node_1"].add_peer("mesh_node_2")
        nodes["mesh_node_2"].add_peer("mesh_node_0")

        # Each node broadcasts its metrics
        for m in nodes.values():
            await m.broadcast_metrics()

        # Each node exchanges anti-entropy digest
        for m in nodes.values():
            digest = m.create_state_digest()
            msg = GossipMessage(
                sender_id=m.local_node_id,
                message_type=GossipMessageType.GOSSIP_DIGEST,
                payload={"digest": digest.to_dict()},
            )
            for other_id, other_m in nodes.items():
                if other_id != m.local_node_id:
                    resp = await other_m.handle_incoming_message(msg)
                    if resp:
                        await m.handle_incoming_message(resp)

        # Verification: all 3 nodes now know about each other
        for nid, m in nodes.items():
            self.assertEqual(m.peer_count, 2, f"{nid} did not discover all 2 other peers")
            for other_id in nodes:
                if other_id != nid:
                    p = m.get_peer(other_id)
                    self.assertIsNotNone(p)
                    self.assertGreater(p.metrics.cpu_percent, 0.0)


if __name__ == "__main__":
    unittest.main()
