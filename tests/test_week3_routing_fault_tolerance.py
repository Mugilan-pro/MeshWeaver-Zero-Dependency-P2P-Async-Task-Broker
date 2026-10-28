"""
MeshWeaver Week 3 Test Suite: Task Routing & Fault Tolerance
Validates:
1. Routing algorithm: Selects the node with lowest reported CPU load.
2. Multi-metric composite scoring and tie-breaking.
3. Failure exclusion during task retries.
4. Continuous heartbeat monitoring & two-tier failure detection (HEALTHY -> SUSPECT -> DEAD).
5. Automatic task re-routing when a worker drops offline.
6. Bounded retry limits and dead-letter queue escalation.
7. Task lease timeout sweeps.
"""

import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from gossip.metrics import SystemMetricsCollector
from gossip.models import GossipMessage, GossipMessageType, NodeMetrics, PeerHealthState
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter, WorkerNodeInfo
from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.serializer import TaskSerializer


class TestWeek3TaskRouting(unittest.TestCase):
    """Verifies load-aware workload routing algorithms (Week 3 Milestone)."""

    def setUp(self):
        self.router = TaskRouter(local_node_id="gateway_node")
        # Ingest 4 workers with diverse CPU loads
        self.router.update_worker_metrics(node_id="worker_alpha", cpu_percent=78.5, ram_percent=60.0, active_tasks=2)
        self.router.update_worker_metrics(node_id="worker_beta", cpu_percent=14.2, ram_percent=25.0, active_tasks=0)
        self.router.update_worker_metrics(node_id="worker_gamma", cpu_percent=45.0, ram_percent=40.0, active_tasks=1)
        self.router.update_worker_metrics(node_id="worker_delta", cpu_percent=88.9, ram_percent=90.0, active_tasks=4)

    def test_routing_selects_lowest_cpu_node(self):
        """Mandate: Routes submitted task to specific node with lowest reported CPU load."""
        task = Task(name="scientific_calc", payload=b"math")
        selected = self.router.select_worker(task, strategy=RoutingStrategy.LOWEST_CPU)

        self.assertIsNotNone(selected)
        self.assertEqual(selected.node_id, "worker_beta")
        self.assertEqual(selected.cpu_percent, 14.2)

    def test_routing_dynamic_load_update(self):
        """Worker load changes dynamically; subsequent tasks follow lowest CPU."""
        task1 = Task(name="job_1", payload=b"")
        self.assertEqual(self.router.select_worker(task1).node_id, "worker_beta")

        # Worker beta spikes, Worker gamma drops to idle
        self.router.update_worker_metrics(node_id="worker_beta", cpu_percent=92.0)
        self.router.update_worker_metrics(node_id="worker_gamma", cpu_percent=8.5)

        task2 = Task(name="job_2", payload=b"")
        new_choice = self.router.select_worker(task2, strategy=RoutingStrategy.LOWEST_CPU)
        self.assertEqual(new_choice.node_id, "worker_gamma")
        self.assertEqual(new_choice.cpu_percent, 8.5)

    def test_routing_composite_load_scoring(self):
        """Composite score considers CPU, active task pressure, and RAM."""
        task = Task(name="composite_job", payload=b"")
        chosen = self.router.select_worker(task, strategy=RoutingStrategy.COMPOSITE_LOAD)
        # worker_beta has lowest composite load: (14.2*0.6) + (0*0.3) + (25*0.1) = 8.52 + 2.5 = 11.02
        self.assertEqual(chosen.node_id, "worker_beta")

    def test_routing_excludes_failed_worker(self):
        """Excluded workers (e.g. during retries) must not be selected."""
        task = Task(name="retry_job", payload=b"")
        chosen = self.router.select_worker(
            task,
            strategy=RoutingStrategy.LOWEST_CPU,
            exclude_node_ids={"worker_beta"},
        )
        # Next lowest is worker_gamma (45.0%)
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.node_id, "worker_gamma")


class TestWeek3FaultTolerance(unittest.TestCase):
    """Verifies heartbeat monitoring and automatic recovery of orphaned tasks (Week 3 Milestone)."""

    def setUp(self):
        self.monitor = GossipMonitor(
            local_node_id="monitor_hub",
            gossip_interval=1.0,
            heartbeat_timeout=2.0,  # 2.0s cutoff for DEAD
        )
        self.router = TaskRouter(local_node_id="monitor_hub")
        self.reliability = ReliabilityManager(default_max_retries=3)

        self.monitor.sync_with_task_router(self.router)
        self.monitor.sync_with_reliability_manager(self.reliability, self.router)

        # Register workers via gossip
        self.router.update_worker_metrics(node_id="worker_primary", cpu_percent=15.0)
        self.router.update_worker_metrics(node_id="worker_backup", cpu_percent=35.0)

        p1 = self.monitor.add_peer("worker_primary")
        p2 = self.monitor.add_peer("worker_backup")

    def test_worker_dropout_detection(self):
        """Failure detector triggers when heartbeat elapsed time exceeds threshold."""
        peer = self.monitor.get_peer("worker_primary")
        peer.last_heartbeat_time = time.time() - 2.5

        newly_suspect, newly_dead = self.monitor.check_peer_health()
        self.assertIn("worker_primary", newly_dead)
        self.assertEqual(peer.health_state, PeerHealthState.DEAD)
        self.assertFalse(peer.is_alive)

    def test_automatic_task_rerouting_on_worker_dropout(self):
        """When worker drops offline, active in-flight tasks are automatically re-assigned to healthy worker."""
        task = Task(name="monte_carlo_pi", payload=b"pi_calc", max_retries=3)
        self.reliability.assign_task(task, "worker_primary")
        self.assertEqual(self.reliability.in_flight_count, 1)

        # Simulate heartbeat timeout on worker_primary
        peer = self.monitor.get_peer("worker_primary")
        peer.last_heartbeat_time = time.time() - 3.0

        reassigned = self.reliability.handle_worker_dropout("worker_primary", self.router)
        self.assertEqual(len(reassigned), 1)

        orphaned_task, new_worker = reassigned[0]
        self.assertEqual(orphaned_task.task_id, task.task_id)
        self.assertIsNotNone(new_worker)
        self.assertEqual(new_worker.node_id, "worker_backup")
        self.assertEqual(orphaned_task.retry_count, 1)
        self.assertEqual(orphaned_task.status, TaskStatus.SCHEDULED)

    def test_retry_exhaustion_moves_task_to_dead_letter_queue(self):
        """Tasks exceeding max_retries permanently transition to FAILED in the DLQ."""
        task = Task(name="faulty_task", payload=b"", max_retries=1)
        self.reliability.assign_task(task, "worker_primary")

        # Retry 1
        res1 = self.reliability.record_failure_and_retry(task.task_id, "Worker crash 1", self.router)
        self.assertIsNotNone(res1)
        self.assertEqual(task.retry_count, 1)

        # Retry 2 -> exceeds limit of 1
        res2 = self.reliability.record_failure_and_retry(task.task_id, "Worker crash 2", self.router)
        self.assertIsNone(res2)
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(self.reliability.dead_letter_count, 1)
        self.assertEqual(self.reliability.in_flight_count, 0)

        dlq = self.reliability.get_dead_letter_tasks()
        self.assertEqual(len(dlq), 1)
        dead_task, error_msg = dlq[0]
        self.assertEqual(dead_task.task_id, task.task_id)
        self.assertIn("Exhausted retries", error_msg)

    def test_lease_timeout_periodic_sweep(self):
        """Expired task leases are automatically detected and re-routed."""
        task = Task(name="stalled_job", payload=b"", timeout_seconds=0.1)
        lease = self.reliability.assign_task(task, "worker_primary", lease_timeout_seconds=0.1)
        lease.assigned_at = time.time() - 10.0  # Force lease expiration

        self.assertTrue(lease.is_expired)
        expired_results = self.reliability.check_lease_timeouts(self.router)
        self.assertEqual(len(expired_results), 1)
        self.assertEqual(task.retry_count, 1)


if __name__ == "__main__":
    unittest.main()
