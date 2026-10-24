"""
Tests for MeshWeaver Scheduler Package
Validates TaskRouter load balancing, healthy worker selection, and ReliabilityManager fault tolerance.
"""

import unittest

from task.models import Task, TaskStatus
from scheduler.router import TaskRouter, WorkerNodeInfo, RoutingStrategy
from scheduler.reliability import ReliabilityManager


class TestSchedulerRouterAndReliability(unittest.TestCase):
    def setUp(self):
        self.router = TaskRouter(local_node_id="node_local")
        self.reliability = ReliabilityManager(default_max_retries=2)

        # Register candidate workers with varied load profiles
        self.router.update_worker_metrics(node_id="node_high_load", cpu_percent=88.0, ram_percent=60.0)
        self.router.update_worker_metrics(node_id="node_medium_load", cpu_percent=42.0, ram_percent=30.0)
        self.router.update_worker_metrics(node_id="node_low_load", cpu_percent=10.5, ram_percent=20.0)

    def test_routing_selects_lowest_cpu_worker(self):
        task = Task(name="benchmark_job", payload=b"")
        chosen = self.router.select_worker(task, strategy=RoutingStrategy.LOWEST_CPU)

        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.node_id, "node_low_load")

    def test_routing_excludes_failed_worker(self):
        task = Task(name="benchmark_job", payload=b"")
        # Exclude node_low_load
        chosen = self.router.select_worker(
            task,
            strategy=RoutingStrategy.LOWEST_CPU,
            exclude_node_ids={"node_low_load"}
        )
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.node_id, "node_medium_load")

    def test_worker_dropout_reassignment(self):
        task = Task(name="critical_job", payload=b"")
        # Assign task to node_low_load
        self.reliability.assign_task(task, "node_low_load")
        self.assertEqual(self.reliability.in_flight_count, 1)

        # Node drops offline (heartbeat lost)
        reassigned = self.reliability.handle_worker_dropout("node_low_load", self.router)

        self.assertEqual(len(reassigned), 1)
        reassigned_task, new_worker = reassigned[0]

        self.assertIsNotNone(new_worker)
        # Reroutes to next lowest CPU node (node_medium_load)
        self.assertEqual(new_worker.node_id, "node_medium_load")
        self.assertEqual(reassigned_task.retry_count, 1)
        self.assertEqual(reassigned_task.status, TaskStatus.SCHEDULED)

    def test_bounded_retry_exhaustion_to_dead_letter(self):
        task = Task(name="failing_job", payload=b"", max_retries=1)
        self.reliability.assign_task(task, "node_high_load")

        # First failure -> retried
        first_retry = self.reliability.record_failure_and_retry(task.task_id, "Error 1", self.router)
        self.assertIsNotNone(first_retry)
        self.assertEqual(task.retry_count, 1)

        # Second failure -> exceeds max_retries of 1
        second_retry = self.reliability.record_failure_and_retry(task.task_id, "Error 2", self.router)
        self.assertIsNone(second_retry)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(self.reliability.dead_letter_count, 1)
        self.assertEqual(self.reliability.in_flight_count, 0)


if __name__ == "__main__":
    unittest.main()
