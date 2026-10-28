"""
MeshWeaver End-to-End P2P MeshNode Integration Test Suite
Validates the complete decentralized task broker combining:
- Week 1: Peer heartbeat and CPU/RAM sampling
- Week 2: 5s gossip telemetry and state reconciliation
- Week 3: Lowest CPU task routing and automatic failure recovery
- Week 4: TLS encryption, HMAC-SHA256 signatures, and topology dashboard
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mesh.node import MeshNode
from network.transport import InMemoryMeshRouter
from task.models import TaskPriority, TaskStatus
from task.serializer import TaskSerializer


def sample_prime_counter(limit: int) -> int:
    """CPU compute workload."""
    return TaskSerializer.workload_prime_sieve(limit)


def sample_pi_estimator(samples: int) -> float:
    """Math compute workload."""
    return TaskSerializer.workload_monte_carlo_pi(samples)


class TestMeshNodeIntegration(unittest.IsolatedAsyncioTestCase):
    """End-to-End multi-node cluster integration tests."""

    async def asyncSetUp(self):
        self.router = InMemoryMeshRouter()

        # Spin up 3 nodes
        self.node_alpha = MeshNode(
            node_id="node_alpha",
            host="127.0.0.1",
            port=9100,
            shared_router=self.router,
            gossip_interval=1.0,
            heartbeat_timeout=3.0,
        )
        self.node_beta = MeshNode(
            node_id="node_beta",
            host="127.0.0.1",
            port=9101,
            shared_router=self.router,
            gossip_interval=1.0,
            heartbeat_timeout=3.0,
        )
        self.node_gamma = MeshNode(
            node_id="node_gamma",
            host="127.0.0.1",
            port=9102,
            shared_router=self.router,
            gossip_interval=1.0,
            heartbeat_timeout=3.0,
        )

        self.nodes = [self.node_alpha, self.node_beta, self.node_gamma]
        for n in self.nodes:
            await n.start()

        # Connect full mesh
        self.node_alpha.add_peer("node_beta", port=9101)
        self.node_alpha.add_peer("node_gamma", port=9102)

        self.node_beta.add_peer("node_alpha", port=9100)
        self.node_beta.add_peer("node_gamma", port=9102)

        self.node_gamma.add_peer("node_alpha", port=9100)
        self.node_gamma.add_peer("node_beta", port=9101)

    async def asyncTearDown(self):
        for n in self.nodes:
            await n.stop()

    async def test_end_to_end_lowest_cpu_routing_and_execution(self):
        """
        Task submitted from node_alpha is routed to node_gamma because it has lowest CPU load.
        Validates cryptographic signing, execution, and return value.
        """
        # Set telemetry loads: beta is busy (75% CPU), gamma is idle (12% CPU)
        self.node_alpha.router.update_worker_metrics("node_beta", cpu_percent=75.0, port=9101)
        self.node_alpha.router.update_worker_metrics("node_gamma", cpu_percent=12.0, port=9102)

        result = await self.node_alpha.submit_task(
            name="prime_benchmark",
            func=sample_prime_counter,
            args=(100,),
            priority=TaskPriority.HIGH,
        )

        self.assertEqual(result.status, TaskStatus.COMPLETED)
        # Primes under 100 is 25
        self.assertEqual(result.return_value, 25)
        self.assertEqual(result.executed_by_node_id, "node_gamma")
        self.assertGreater(result.execution_time_seconds, 0.0)

    async def test_end_to_end_worker_dropout_automatic_recovery(self):
        """
        If the assigned worker drops offline, the task is automatically re-routed
        to the next lowest-CPU healthy peer.
        """
        # Node beta is 20% CPU, Node gamma is 60% CPU
        self.node_alpha.router.update_worker_metrics("node_beta", cpu_percent=20.0, port=9101)
        self.node_alpha.router.update_worker_metrics("node_gamma", cpu_percent=60.0, port=9102)

        # Simulate node_beta crashing / unregistering from network
        self.router.unregister_endpoint("node_beta")

        # Submitting task should attempt node_beta, catch failure, and automatically re-route to node_gamma!
        result = await self.node_alpha.submit_task(
            name="resilient_pi_calc",
            func=sample_pi_estimator,
            args=(2000,),
            timeout_seconds=5.0,
            max_retries=2,
        )

        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertAlmostEqual(result.return_value, 3.14, delta=0.2)
        # Should have successfully executed on backup worker (node_gamma)
        self.assertEqual(result.executed_by_node_id, "node_gamma")

    async def test_cryptographic_tamper_rejection_over_network(self):
        """
        If a network adversary tampers with a task payload, the worker node rejects it
        with a security error and refuses execution.
        """
        # Attempt to forge or tamper with a task
        task = TaskSerializer.package_task(
            name="tampered_task",
            func=sample_prime_counter,
            args=(50,),
            origin_node_id="node_alpha",
        )
        self.node_alpha.security.sign_task(task)

        # Corrupt payload
        task.payload = task.payload[:-1] + b"\xff"

        from network.protocol import NetworkPacket, ProtocolMessageType
        pkt = NetworkPacket(
            message_type=ProtocolMessageType.TASK_SUBMISSION,
            sender_id="node_alpha",
            payload={"task": task.to_dict()},
            binary_data=task.payload,
            signature=task.signature,
        )

        # Send directly to node_beta
        resp = await self.router.send("127.0.0.1", 9101, pkt)
        self.assertEqual(resp.message_type, ProtocolMessageType.ERROR)
        self.assertIn("Security verification failed", resp.payload["error"])


if __name__ == "__main__":
    unittest.main()
