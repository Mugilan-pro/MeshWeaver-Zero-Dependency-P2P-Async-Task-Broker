"""
MeshWeaver Task Execution Engine
Asynchronously executes compute workloads without blocking the asyncio event loop.
Offloads CPU-bound tasks to worker pools, enforces execution timeouts,
and records precise performance metrics for gossip telemetry.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging
import os
import time
import traceback
from typing import Any, Callable, Dict, Optional, Tuple

from .models import Task, TaskResult, TaskStatus
from .serializer import TaskSerializer

logger = logging.getLogger("MeshWeaver.TaskExecutor")


def _execute_in_worker_pool(packed_payload: bytes, secret_key: Optional[bytes] = None) -> Any:
    """
    Subprocess / Worker thread entry point.
    Unpacks function, validates checksum, and executes callable with arguments.
    """
    func, args, kwargs = TaskSerializer.deserialize_callable(packed_payload, secret_key=secret_key)
    return func(*args, **kwargs)


class TaskExecutor:
    """
    Asynchronous compute execution engine for MeshWeaver nodes.
    Maintains concurrency counters to supply live load metrics to the scheduler and gossip engine.
    """

    def __init__(
        self,
        node_id: Optional[str] = None,
        max_workers: Optional[int] = None,
        use_process_pool: bool = False,
        secret_key: Optional[bytes] = None,
    ) -> None:
        self.node_id = node_id or f"node_{os.getpid()}"
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.use_process_pool = use_process_pool
        self.secret_key = secret_key

        # Concurrency & Metric tracking
        self._active_tasks: Dict[str, Task] = {}
        self._completed_count = 0
        self._failed_count = 0
        self._total_execution_time = 0.0

        # Dedicated executor pool for task workloads
        if self.use_process_pool:
            self._pool = ProcessPoolExecutor(max_workers=self.max_workers)
        else:
            self._pool = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix="MeshWeaver-Worker",
            )
        self._is_shutdown = False

    @property
    def active_task_count(self) -> int:
        """Current number of active running tasks on this node."""
        return len(self._active_tasks)

    @property
    def total_completed(self) -> int:
        """Total successful executions on this node."""
        return self._completed_count

    @property
    def total_failed(self) -> int:
        """Total failed executions on this node."""
        return self._failed_count

    @property
    def average_execution_time(self) -> float:
        """Average runtime per task on this node."""
        total = self._completed_count + self._failed_count
        return (self._total_execution_time / total) if total > 0 else 0.0

    async def execute(self, task: Task) -> TaskResult:
        """
        Asynchronously executes a Task object.
        Enforces timeout bounds and ensures the event loop remains responsive.
        """
        if self._is_shutdown:
            return TaskResult.failure(
                task_id=task.task_id,
                error_message="TaskExecutor is shut down",
                executed_by_node_id=self.node_id,
            )

        task.mark_running()
        self._active_tasks[task.task_id] = task
        start_time = time.perf_counter()

        logger.info(f"[{self.node_id}] Executing task '{task.name}' (ID: {task.task_id[:8]})")

        try:
            try:
                loop = asyncio.get_running_loop()
            except (RuntimeError, AttributeError):
                loop = asyncio.get_event_loop()
            
            # Offload synchronous execution to thread or process pool
            result_future = loop.run_in_executor(
                self._pool,
                _execute_in_worker_pool,
                task.payload,
                self.secret_key,
            )

            raw_return = await asyncio.wait_for(
                result_future,
                timeout=task.timeout_seconds,
            )

            elapsed = time.perf_counter() - start_time
            self._completed_count += 1
            self._total_execution_time += elapsed

            task.mark_completed()
            result_payload = TaskSerializer.serialize_value(raw_return)

            logger.info(
                f"[{self.node_id}] Completed task '{task.name}' in {elapsed:.3f}s"
            )

            return TaskResult.success(
                task_id=task.task_id,
                result_payload=result_payload,
                return_value=raw_return,
                execution_time_seconds=elapsed,
                executed_by_node_id=self.node_id,
            )

        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - start_time
            self._failed_count += 1
            self._total_execution_time += elapsed
            task.status = TaskStatus.TIMEOUT
            err_msg = f"Task exceeded timeout bound of {task.timeout_seconds:.1f}s"
            logger.warning(f"[{self.node_id}] {err_msg} (task: {task.task_id[:8]})")

            return TaskResult.failure(
                task_id=task.task_id,
                error_message=err_msg,
                execution_time_seconds=elapsed,
                executed_by_node_id=self.node_id,
                is_timeout=True,
            )

        except Exception as e:
            elapsed = time.perf_counter() - start_time
            self._failed_count += 1
            self._total_execution_time += elapsed
            task.mark_failed()
            tb = traceback.format_exc()
            err_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"[{self.node_id}] Execution error in task {task.task_id[:8]}: {err_msg}")

            return TaskResult.failure(
                task_id=task.task_id,
                error_message=err_msg,
                traceback=tb,
                execution_time_seconds=elapsed,
                executed_by_node_id=self.node_id,
            )

        finally:
            self._active_tasks.pop(task.task_id, None)

    async def execute_direct(
        self,
        func: Callable[..., Any],
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout_seconds: float = 30.0,
        task_name: str = "direct_call",
    ) -> TaskResult:
        """Convenience direct invocation wrapper."""
        task = TaskSerializer.package_task(
            name=task_name,
            func=func,
            args=args,
            kwargs=kwargs,
            timeout_seconds=timeout_seconds,
            origin_node_id=self.node_id,
        )
        return await self.execute(task)

    def shutdown(self, wait: bool = True) -> None:
        """Gracefully terminates worker pools."""
        self._is_shutdown = True
        self._pool.shutdown(wait=wait)
        logger.info(f"[{self.node_id}] TaskExecutor terminated cleanly.")
