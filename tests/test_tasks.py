"""
Tests for MeshWeaver Task Package
Validates Task/TaskResult lifecycle, serialization integrity, and asynchronous execution.
Compatible with Python 3.7+ through 3.13+.
"""

import asyncio
import time
import unittest

from task.models import Task, TaskResult, TaskStatus, TaskPriority
from task.serializer import TaskSerializer, IntegrityError
from task.executor import TaskExecutor


def sample_square(x: int) -> int:
    return x * x


def sample_faulty_func() -> None:
    raise ValueError("Simulated compute failure")


def sample_slow_func(delay: float) -> str:
    time.sleep(delay)
    return "done"


class TestTaskModels(unittest.TestCase):
    def test_task_lifecycle_transitions(self):
        task = Task(name="test_job", payload=b"payload_bytes")
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertTrue(task.can_retry())

        task.mark_scheduled("node_01")
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.assigned_node_id, "node_01")

        task.mark_running()
        self.assertEqual(task.status, TaskStatus.RUNNING)
        self.assertIsNotNone(task.started_at)

        task.mark_completed()
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertIsNotNone(task.completed_at)

    def test_task_dict_roundtrip(self):
        original = Task(name="matrix_calc", payload=b"\x01\x02\x03", priority=TaskPriority.HIGH)
        as_dict = original.to_dict()
        restored = Task.from_dict(as_dict)

        self.assertEqual(original.task_id, restored.task_id)
        self.assertEqual(original.name, restored.name)
        self.assertEqual(original.payload, restored.payload)
        self.assertEqual(original.priority, restored.priority)


class TestTaskSerializer(unittest.TestCase):
    def test_serialize_and_deserialize_callable(self):
        packed = TaskSerializer.serialize_callable(sample_square, args=(5,))
        func, args, kwargs = TaskSerializer.deserialize_callable(packed)
        res = func(*args, **kwargs)
        self.assertEqual(res, 25)

    def test_tamper_detection(self):
        packed = TaskSerializer.serialize_callable(sample_square, args=(7,))
        corrupted = packed[:-1] + b"\x00"
        with self.assertRaises(IntegrityError):
            TaskSerializer.deserialize_callable(corrupted)

    def test_benchmark_task_generation(self):
        task = TaskSerializer.create_benchmark_task("primes", 50)
        self.assertEqual(task.name, "benchmark_primes")
        func, args, _ = TaskSerializer.deserialize_callable(task.payload)
        primes_count = func(*args)
        # Primes under 50: 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47 = 15
        self.assertEqual(primes_count, 15)


class TestTaskExecutor(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.executor = TaskExecutor(node_id="test_worker_node", max_workers=2)

    def tearDown(self):
        self.executor.shutdown(wait=True)
        self.loop.close()

    def run_async(self, coro):
        """Helper to run async coroutines across Python 3.7+"""
        return self.loop.run_until_complete(coro)

    def test_successful_execution(self):
        task = TaskSerializer.package_task("square_task", sample_square, args=(9,))
        result = self.run_async(self.executor.execute(task))

        self.assertEqual(result.status, TaskStatus.COMPLETED)
        self.assertEqual(result.return_value, 81)
        self.assertEqual(result.executed_by_node_id, "test_worker_node")
        self.assertGreater(result.execution_time_seconds, 0.0)
        self.assertEqual(self.executor.total_completed, 1)

    def test_exception_handling(self):
        task = TaskSerializer.package_task("faulty_task", sample_faulty_func)
        result = self.run_async(self.executor.execute(task))

        self.assertEqual(result.status, TaskStatus.FAILED)
        self.assertIn("ValueError", result.error_message)
        self.assertIsNotNone(result.traceback)
        self.assertEqual(self.executor.total_failed, 1)

    def test_timeout_enforcement(self):
        task = TaskSerializer.package_task("slow_task", sample_slow_func, args=(1.0,), timeout_seconds=0.2)
        result = self.run_async(self.executor.execute(task))

        self.assertEqual(result.status, TaskStatus.TIMEOUT)
        self.assertIn("timeout bound", result.error_message)


if __name__ == "__main__":
    unittest.main()
