"""
Integration Tests for MeshWeaver Unified MeshNode
Tests end-to-end TLS socket execution, lowest-CPU routing across network,
cryptographic signature verification, tamper rejection, and dashboard rendering.
"""

import asyncio
import unittest

from node import MeshNode
from task.models import TaskPriority, TaskStatus
from task.serializer import TaskSerializer
from scheduler.router import RoutingStrategy


def sample_square(x: int) -> int:
    return x * x


def sample_faulty() -> None:
    raise ValueError("Intentional compute crash")


class TestMeshNodeIntegration(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def run_async(self, coro):
        return self.loop.run_until_complete(coro)

    def test_tls_encrypted_task_execution(self):
        async def _test():
            node1 = MeshNode(port=0, dht_port=0, enable_tls=True, require_signatures=True)
            node2 = MeshNode(port=0, dht_port=0, enable_tls=True, require_signatures=True)
            await node1.start()
            await node2.start()

            try:
                # Dispatch signed task from node1 to node2 over TLS
                task = TaskSerializer.package_task(
                    name="square_task",
                    func=sample_square,
                    args=(12,),
                    origin_node_id=node1.node_id_hex,
                )

                result = await node1.client.send_task(
                    task=task,
                    target_host=node2.host,
                    target_port=node2.port,
                    origin_node_id=node1.node_id_hex,
                )

                self.assertEqual(result.status, TaskStatus.COMPLETED)
                self.assertEqual(result.return_value, 144)
                self.assertEqual(result.executed_by_node_id, node2.node_id_hex)
            finally:
                await node1.stop()
                await node2.stop()

        self.run_async(_test())

    def test_lowest_cpu_routing_decision(self):
        async def _test():
            coordinator = MeshNode(port=0, dht_port=0, enable_tls=False)
            worker_high = MeshNode(port=0, dht_port=0, enable_tls=False)
            worker_low = MeshNode(port=0, dht_port=0, enable_tls=False)

            await coordinator.start()
            await worker_high.start()
            await worker_low.start()

            try:
                # Ingest gossip load
                coordinator.router.update_worker_metrics(coordinator.node_id_hex, cpu_percent=50.0)
                worker_high.metrics_collector.set_simulation_override(cpu_percent=85.0)
                worker_low.metrics_collector.set_simulation_override(cpu_percent=12.0)

                coordinator.gossip.handle_incoming_gossip(worker_high.metrics_collector.capture_snapshot().to_dict())
                coordinator.gossip.handle_incoming_gossip(worker_low.metrics_collector.capture_snapshot().to_dict())

                # Task should route to worker_low
                task = TaskSerializer.package_task(
                    name="calc_task",
                    func=sample_square,
                    args=(7,),
                    origin_node_id=coordinator.node_id_hex,
                )

                chosen = coordinator.router.select_worker(task, strategy=RoutingStrategy.LOWEST_CPU)
                self.assertIsNotNone(chosen)
                self.assertEqual(chosen.node_id, worker_low.node_id_hex)

                # Submit task end-to-end
                result = await coordinator.submit_packaged_task(task, strategy=RoutingStrategy.LOWEST_CPU)
                self.assertEqual(result.status, TaskStatus.COMPLETED)
                self.assertEqual(result.return_value, 49)
                self.assertEqual(result.executed_by_node_id, worker_low.node_id_hex)
            finally:
                await coordinator.stop()
                await worker_high.stop()
                await worker_low.stop()

        self.run_async(_test())

    def test_security_rejection_of_unauthenticated_request(self):
        async def _test():
            worker = MeshNode(port=0, dht_port=0, enable_tls=False, require_signatures=True)
            await worker.start()

            try:
                task = TaskSerializer.package_task(
                    name="unauth_task",
                    func=sample_square,
                    args=(5,),
                    origin_node_id="attacker_node",
                )

                # Craft forged/tampered auth metadata
                bogus_auth = {
                    "signature": "00" * 64,
                    "public_key": "11" * 32,
                    "timestamp": 123456.0,
                    "nonce": "bogus_nonce",
                }

                # Attempt to directly execute with forged auth
                from network.protocol import MessageType
                request_frame = {
                    "type": MessageType.TASK_EXECUTE_REQUEST,
                    "task_id": task.task_id,
                    "name": task.name,
                    "origin_node_id": "attacker_node",
                    "payload_hex": task.payload.hex(),
                    "priority": task.priority.value,
                    "timeout_seconds": 10.0,
                    "max_retries": 1,
                    "auth": bogus_auth,
                }

                response = await worker.server._process_task_request(request_frame)
                self.assertEqual(response.get("status"), TaskStatus.FAILED.value)
                self.assertTrue(response.get("is_security_failure"))
                self.assertIn("Security Verification Failed", response.get("error_message", ""))
                self.assertEqual(worker.server.signatures_rejected, 1)
            finally:
                await worker.stop()

        self.run_async(_test())

    def test_dashboard_snapshot_generation(self):
        node = MeshNode(port=0, dht_port=0, enable_tls=True)
        snapshot = node.get_dashboard_snapshot()

        self.assertIn("node_info", snapshot)
        self.assertIn("dht_peers", snapshot)
        self.assertIn("cluster_peers", snapshot)
        self.assertIn("task_info", snapshot)
        self.assertTrue(snapshot["node_info"]["tls_enabled"])
        self.assertEqual(snapshot["node_info"]["signature_algorithm"], "Ed25519")


if __name__ == "__main__":
    unittest.main()
