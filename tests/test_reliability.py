"""
Tests for MeshWeaver Reliability Subsystem
Validates task lease management, worker dropout recovery, bounded retries,
dead-letter queue (DLQ) operations, and automated watchdog sweeps.
Compatible with Python 3.7+ through 3.13+.
"""

import asyncio
import time
import unittest

from task.models import Task, TaskResult, TaskStatus
from scheduler.router import TaskRouter, WorkerNodeInfo
from scheduler.reliability import ReliabilityManager


class TestReliabilitySubsystem(unittest.TestCase):
    def setUp(self):
        self.router = TaskRouter(local_node_id="node_local")
        self.reliability = ReliabilityManager(default_max_retries=2, default_lease_timeout=1.0)

        # Register two healthy candidate workers
        self.router.update_worker_metrics(node_id="worker_primary", cpu_percent=15.0, ram_percent=20.0)
        self.router.update_worker_metrics(node_id="worker_secondary", cpu_percent=45.0, ram_percent=35.0)

    def test_lease_assignment_and_success(self):
        task = Task(name="test_compute", payload=b"dummy_data")
        assignment = self.reliability.assign_task(task, "worker_primary", lease_timeout_seconds=5.0)

        self.assertEqual(assignment.worker_node_id, "worker_primary")
        self.assertEqual(self.reliability.in_flight_count, 1)
        self.assertFalse(assignment.is_expired)

        # Record success
        res = TaskResult.success(task_id=task.task_id, return_value=42)
        completed_task = self.reliability.record_success(task.task_id, res)

        self.assertIsNotNone(completed_task)
        self.assertEqual(completed_task.status, TaskStatus.COMPLETED)
        self.assertEqual(self.reliability.in_flight_count, 0)
        self.assertEqual(self.reliability.get_stats()["completed"], 1)

    def test_worker_dropout_recovery(self):
        task = Task(name="urgent_task", payload=b"payload")
        self.reliability.assign_task(task, "worker_primary")

        # worker_primary suddenly drops offline!
        reassigned = self.reliability.handle_worker_dropout("worker_primary", self.router)

        self.assertEqual(len(reassigned), 1)
        reassigned_task, new_worker = reassigned[0]

        self.assertIsNotNone(new_worker)
        self.assertEqual(new_worker.node_id, "worker_secondary")
        self.assertEqual(reassigned_task.retry_count, 1)
        self.assertEqual(reassigned_task.status, TaskStatus.SCHEDULED)
        # Old worker is in failed history
        assignment = self.reliability._active_leases[task.task_id]
        self.assertIn("worker_primary", assignment.failed_node_ids)

    def test_bounded_retry_and_dead_letter_queue(self):
        task = Task(name="failing_task", payload=b"bad_payload", max_retries=1)
        self.reliability.assign_task(task, "worker_primary")

        # First failure -> retried to worker_secondary
        first_retry = self.reliability.record_failure_and_retry(task.task_id, "Crash 1", self.router)
        self.assertIsNotNone(first_retry)
        self.assertEqual(task.retry_count, 1)

        # Second failure -> exceeds max_retries (1)
        second_retry = self.reliability.record_failure_and_retry(task.task_id, "Crash 2", self.router)
        self.assertIsNone(second_retry)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(self.reliability.dead_letter_count, 1)
        self.assertEqual(self.reliability.in_flight_count, 0)

        # Verify DLQ structured representation
        dlq = self.reliability.get_dead_letter_queue()
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0]["task_id"], task.task_id)

    def test_requeue_dead_letter_task(self):
        task = Task(name="rescue_task", payload=b"rescue", max_retries=1)
        self.reliability.assign_task(task, "worker_primary")

        # Force fail into DLQ
        self.reliability.record_failure_and_retry(task.task_id, "Fail", self.router)
        self.reliability.record_failure_and_retry(task.task_id, "Fail", self.router)
        self.assertEqual(self.reliability.dead_letter_count, 1)

        # Admin rescues task from DLQ
        rescued = self.reliability.requeue_dead_letter_task(task.task_id, self.router, reset_retries=True)
        self.assertIsNotNone(rescued)
        rescued_task, target_worker = rescued
        self.assertEqual(rescued_task.retry_count, 0)
        self.assertEqual(self.reliability.dead_letter_count, 0)
        self.assertEqual(self.reliability.in_flight_count, 1)

    def test_exponential_backoff_bounds(self):
        for attempt in range(5):
            delay = self.reliability.calculate_backoff(attempt)
            self.assertGreaterEqual(delay, 0.0)
            self.assertLessEqual(delay, self.reliability.max_retry_backoff)

    def test_active_leases_summary(self):
        task = Task(name="monitoring_task", payload=b"abc")
        self.reliability.assign_task(task, "worker_primary")
        summary = self.reliability.get_active_leases_summary()
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["task_id"], task.task_id)
        self.assertEqual(summary[0]["worker_node_id"], "worker_primary")


class TestReliabilityWatchdogAsync(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.router = TaskRouter(local_node_id="node_local")
        self.router.update_worker_metrics(node_id="node_a", cpu_percent=10.0)
        self.router.update_worker_metrics(node_id="node_b", cpu_percent=20.0)
        self.reliability = ReliabilityManager(default_max_retries=2, default_lease_timeout=0.2)

    def tearDown(self):
        self.reliability.stop_lease_watchdog()
        self.loop.close()

    def test_lease_expiration_and_sweep(self):
        async def _test_coro():
            task = Task(name="expiring_task", payload=b"xyz")
            # Assign with extremely short lease timeout (0.1s)
            assignment = self.reliability.assign_task(task, "node_a", lease_timeout_seconds=0.1)

            # Sleep 0.2s so lease expires
            await asyncio.sleep(0.2)
            self.assertTrue(assignment.is_expired)

            # Manual or watchdog sweep triggers re-assignment to node_b
            expired_reassignments = self.reliability.check_lease_timeouts(self.router)
            self.assertEqual(len(expired_reassignments), 1)
            reassigned_task, new_worker = expired_reassignments[0]
            self.assertEqual(new_worker.node_id, "node_b")
            self.assertEqual(reassigned_task.retry_count, 1)

        self.loop.run_until_complete(_test_coro())


if __name__ == "__main__":
    unittest.main()
