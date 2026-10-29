"""
MeshWeaver Reliability Subsystem
Manages in-flight task leases, detects node dropouts, enforces bounded retry limits,
provides exponential backoff jitter, and automatically reassigns orphaned tasks to healthy peers.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple, Any

from task.models import Task, TaskResult, TaskStatus
from .router import TaskRouter, WorkerNodeInfo

logger = logging.getLogger("MeshWeaver.ReliabilityManager")


@dataclass
class FailureRecord:
    """Historical record of an individual execution failure attempt."""
    worker_node_id: str
    failed_at: float
    reason: str


@dataclass
class TaskAssignment:
    """Tracks an active execution lease of a task assigned to a worker node."""
    task: Task
    worker_node_id: str
    assigned_at: float = field(default_factory=time.time)
    lease_timeout_seconds: float = 35.0
    failed_node_ids: Set[str] = field(default_factory=set)
    failure_history: List[FailureRecord] = field(default_factory=list)

    @property
    def is_expired(self) -> bool:
        """Indicates whether the task lease duration has expired without completion."""
        return (time.time() - self.assigned_at) > self.lease_timeout_seconds

    @property
    def remaining_lease_seconds(self) -> float:
        """Remaining seconds before the current lease expires."""
        elapsed = time.time() - self.assigned_at
        return max(0.0, self.lease_timeout_seconds - elapsed)


class ReliabilityManager:
    """
    Guarantees bounded, resilient execution for distributed tasks.
    Coordinates with TaskRouter to recover from worker dropouts, timeouts, and network partitions.
    """

    def __init__(
        self,
        default_max_retries: int = 3,
        default_lease_timeout: float = 30.0,
        base_retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
    ) -> None:
        self.default_max_retries = default_max_retries
        self.default_lease_timeout = default_lease_timeout
        self.base_retry_backoff = base_retry_backoff
        self.max_retry_backoff = max_retry_backoff

        # In-flight task tracking: task_id -> TaskAssignment
        self._active_leases: Dict[str, TaskAssignment] = {}
        # Completed results: task_id -> TaskResult
        self._completed_results: Dict[str, TaskResult] = {}
        # Dead-letter queue for tasks that exhausted retry bounds: task_id -> (Task, reason)
        self._dead_letter_tasks: Dict[str, Tuple[Task, str]] = {}

        # Observability callback hooks
        self.on_task_reassigned: Optional[Callable[[Task, str, str], None]] = None
        self.on_task_exhausted: Optional[Callable[[Task, str], None]] = None

        # Background watchdog task reference
        self._watchdog_task: Optional[asyncio.Task] = None
        self._is_running = False

    @property
    def in_flight_count(self) -> int:
        """Current number of active tasks distributed across the mesh."""
        return len(self._active_leases)

    @property
    def dead_letter_count(self) -> int:
        """Number of tasks that exhausted all retry attempts."""
        return len(self._dead_letter_tasks)

    def calculate_backoff(self, retry_count: int) -> float:
        """Calculates exponential backoff with full jitter to avoid thundering herd."""
        exponential = min(self.max_retry_backoff, self.base_retry_backoff * (2 ** retry_count))
        return random.uniform(0.0, exponential)

    def assign_task(
        self,
        task: Task,
        worker_node_id: str,
        lease_timeout_seconds: Optional[float] = None,
    ) -> TaskAssignment:
        """Records a new lease assignment of a task to a worker node."""
        if lease_timeout_seconds is not None:
            lease_timeout = float(lease_timeout_seconds)
        else:
            timeout = task.timeout_seconds or self.default_lease_timeout
            lease_timeout = timeout + 5.0

        task.mark_scheduled(worker_node_id)
        assignment = self._active_leases.get(task.task_id)
        if assignment:
            assignment.worker_node_id = worker_node_id
            assignment.assigned_at = time.time()
            assignment.lease_timeout_seconds = lease_timeout
        else:
            assignment = TaskAssignment(
                task=task,
                worker_node_id=worker_node_id,
                assigned_at=time.time(),
                lease_timeout_seconds=lease_timeout,
            )
            self._active_leases[task.task_id] = assignment

        logger.info(
            f"Assigned task {task.task_id[:8]} to worker {worker_node_id} "
            f"(attempt {task.retry_count + 1}/{task.max_retries + 1})"
        )
        return assignment

    def record_success(self, task_id: str, result: TaskResult) -> Optional[Task]:
        """
        Marks an in-flight task as successfully completed.
        Releases the lease and stores the result.
        """
        assignment = self._active_leases.pop(task_id, None)
        if assignment:
            assignment.task.mark_completed()
            self._completed_results[task_id] = result
            logger.info(f"Task {task_id[:8]} completed successfully by {assignment.worker_node_id}")
            return assignment.task
        return None

    def record_failure_and_retry(
        self,
        task_id: str,
        reason: str,
        router: TaskRouter,
    ) -> Optional[Tuple[Task, WorkerNodeInfo]]:
        """
        Handles execution failure or timeout.
        If bounded retries remain, reassigns to an alternative healthy worker.
        Otherwise moves task to dead-letter queue.
        """
        assignment = self._active_leases.get(task_id)
        if not assignment:
            logger.warning(f"Attempted to process failure for unknown task {task_id[:8]}")
            return None

        task = assignment.task
        failed_worker_id = assignment.worker_node_id
        assignment.failed_node_ids.add(failed_worker_id)
        assignment.failure_history.append(
            FailureRecord(worker_node_id=failed_worker_id, failed_at=time.time(), reason=reason)
        )

        if task.can_retry():
            task.increment_retry()
            logger.warning(
                f"Task {task.task_id[:8]} failed on {failed_worker_id} ({reason}). "
                f"Initiating retry {task.retry_count}/{task.max_retries}..."
            )

            # Route to an alternative healthy worker, excluding previously failed nodes
            new_worker = router.select_worker(task, exclude_node_ids=assignment.failed_node_ids)

            if new_worker:
                self.assign_task(task, new_worker.node_id)
                if self.on_task_reassigned:
                    self.on_task_reassigned(task, failed_worker_id, new_worker.node_id)
                return task, new_worker
            else:
                logger.error(f"No alternative healthy workers available for task {task.task_id[:8]}")
                return None
        else:
            task.mark_failed()
            self._active_leases.pop(task_id, None)
            self._dead_letter_tasks[task_id] = (task, f"Exhausted retries ({task.max_retries}). Last error: {reason}")
            logger.critical(
                f"Task {task.task_id[:8]} permanently FAILED after {task.retry_count} retries. Moved to dead-letter queue."
            )
            if self.on_task_exhausted:
                self.on_task_exhausted(task, reason)
            return None

    def handle_worker_dropout(
        self,
        dead_node_id: str,
        router: TaskRouter,
    ) -> List[Tuple[Task, Optional[WorkerNodeInfo]]]:
        """
        Invoked when heartbeat monitors report a node dropout.
        Identifies orphaned tasks on the dead node and initiates instant reassignment.
        """
        router.mark_worker_dead(dead_node_id)
        reassigned: List[Tuple[Task, Optional[WorkerNodeInfo]]] = []

        orphaned_task_ids = [
            task_id
            for task_id, assignment in self._active_leases.items()
            if assignment.worker_node_id == dead_node_id
        ]

        logger.warning(
            f"Node {dead_node_id} dropped from mesh. Re-routing {len(orphaned_task_ids)} orphaned tasks..."
        )

        for task_id in orphaned_task_ids:
            recovery_result = self.record_failure_and_retry(
                task_id=task_id,
                reason=f"Assigned node {dead_node_id} dropped offline (heartbeat timeout)",
                router=router,
            )
            if recovery_result:
                reassigned.append(recovery_result)
            else:
                assignment = self._active_leases.get(task_id)
                if assignment:
                    reassigned.append((assignment.task, None))

        return reassigned

    def check_lease_timeouts(
        self,
        router: TaskRouter,
    ) -> List[Tuple[Task, Optional[WorkerNodeInfo]]]:
        """Periodic sweep for task leases that timed out without result response."""
        expired_ids = [
            t_id for t_id, assignment in self._active_leases.items()
            if assignment.is_expired
        ]
        results: List[Tuple[Task, Optional[WorkerNodeInfo]]] = []
        for t_id in expired_ids:
            res = self.record_failure_and_retry(
                task_id=t_id,
                reason="Task lease expired without response from worker",
                router=router,
            )
            if res:
                results.append(res)
        return results

    async def start_lease_watchdog(
        self,
        router: TaskRouter,
        check_interval_seconds: float = 1.0,
    ) -> None:
        """
        Asynchronous background loop that automatically scans for expired leases
        and recovers orphaned tasks without manual polling.
        """
        self._is_running = True
        logger.info("ReliabilityManager watchdog started.")
        while self._is_running:
            try:
                expired = self.check_lease_timeouts(router)
                if expired:
                    logger.info(f"Watchdog auto-reassigned {len(expired)} expired task leases.")
            except Exception as e:
                logger.error(f"Error during reliability watchdog sweep: {e}")
            await asyncio.sleep(check_interval_seconds)

    def stop_lease_watchdog(self) -> None:
        """Halts the background lease watchdog."""
        self._is_running = False
        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
        logger.info("ReliabilityManager watchdog stopped.")

    def requeue_dead_letter_task(
        self,
        task_id: str,
        router: TaskRouter,
        reset_retries: bool = True,
    ) -> Optional[Tuple[Task, WorkerNodeInfo]]:
        """Allows manual administrative rescue of a task stranded in the Dead-Letter Queue."""
        entry = self._dead_letter_tasks.pop(task_id, None)
        if not entry:
            return None
        task, _ = entry
        if reset_retries:
            task.retry_count = 0
            task.status = TaskStatus.PENDING

        worker = router.select_worker(task)
        if worker:
            self.assign_task(task, worker.node_id)
            return task, worker
        return None

    def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Returns structured view of tasks in the Dead-Letter Queue."""
        return [
            {
                "task_id": task.task_id,
                "name": task.name,
                "reason": reason,
                "retry_count": task.retry_count,
                "created_at": task.created_at,
            }
            for task_id, (task, reason) in self._dead_letter_tasks.items()
        ]

    def get_active_leases_summary(self) -> List[Dict[str, Any]]:
        """Returns telemetry overview of in-flight task leases."""
        return [
            {
                "task_id": assignment.task.task_id,
                "name": assignment.task.name,
                "worker_node_id": assignment.worker_node_id,
                "remaining_seconds": round(assignment.remaining_lease_seconds, 1),
                "retry_count": assignment.task.retry_count,
                "failed_nodes": list(assignment.failed_node_ids),
            }
            for assignment in self._active_leases.values()
        ]

    def get_stats(self) -> Dict[str, int]:
        """Returns snapshot metrics for monitoring dashboards."""
        return {
            "in_flight": len(self._active_leases),
            "completed": len(self._completed_results),
            "dead_letter": len(self._dead_letter_tasks),
        }
