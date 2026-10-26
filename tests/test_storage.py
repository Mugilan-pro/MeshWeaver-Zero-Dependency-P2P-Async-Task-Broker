"""
Unit tests for MeshWeaver SQLite Persistent Storage Subsystem.
"""

import os
import tempfile
import threading
import unittest

from storage.db import MeshWeaverStorage


class TestMeshWeaverStorage(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "test_meshweaver.db")
        self.storage = MeshWeaverStorage(self.db_path)

    def tearDown(self):
        self.storage.close()
        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except Exception:
                pass

    def test_save_and_get_task(self):
        self.storage.save_task(
            task_id="task_001",
            name="MonteCarloPi",
            status="COMPLETED",
            priority=2,
            assigned_worker="worker_pi_01",
            execution_time_ms=45.2,
            result=3.14159,
        )

        task = self.storage.get_task("task_001")
        self.assertIsNotNone(task)
        self.assertEqual(task["task_id"], "task_001")
        self.assertEqual(task["task_name"], "MonteCarloPi")
        self.assertEqual(task["status"], "COMPLETED")
        self.assertEqual(task["worker_node_id"], "worker_pi_01")
        self.assertEqual(task["result"], 3.14159)
        self.assertAlmostEqual(task["execution_time_ms"], 45.2)

    def test_update_existing_task(self):
        self.storage.save_task(
            task_id="task_002",
            name="PrimeSieve",
            status="RUNNING",
            assigned_worker="worker_01",
        )
        task = self.storage.get_task("task_002")
        self.assertEqual(task["status"], "RUNNING")

        # Update upon completion
        self.storage.save_task(
            task_id="task_002",
            name="PrimeSieve",
            status="COMPLETED",
            assigned_worker="worker_01",
            execution_time_ms=12.5,
            result=9592,
        )
        updated = self.storage.get_task("task_002")
        self.assertEqual(updated["status"], "COMPLETED")
        self.assertEqual(updated["result"], 9592)
        self.assertEqual(updated["execution_time_ms"], 12.5)

    def test_recent_tasks_and_count(self):
        for i in range(5):
            self.storage.save_task(
                task_id=f"t_{i}",
                name="Benchmark",
                status="COMPLETED" if i % 2 == 0 else "FAILED",
                execution_time_ms=i * 10.0,
            )

        self.assertEqual(self.storage.count_tasks(), 5)
        self.assertEqual(self.storage.count_tasks(status="COMPLETED"), 3)
        self.assertEqual(self.storage.count_tasks(status="FAILED"), 2)

        recent = self.storage.get_recent_tasks(limit=3)
        self.assertEqual(len(recent), 3)

    def test_audit_events_logging(self):
        self.storage.save_event("INFO", "Registered worker_01")
        self.storage.save_event("ALERT", "Lost heartbeat from worker_02")
        self.storage.save_event("GOSSIP", "Gossip round completed")

        events = self.storage.get_recent_events(limit=10)
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]["level"], "INFO")
        self.assertEqual(events[1]["level"], "ALERT")
        self.assertEqual(events[2]["level"], "GOSSIP")

        alert_events = self.storage.get_recent_events(limit=10, level="ALERT")
        self.assertEqual(len(alert_events), 1)
        self.assertIn("Lost heartbeat", alert_events[0]["message"])

    def test_node_telemetry_snapshot(self):
        self.storage.record_node_snapshot(
            node_id="worker_01",
            cpu_percent=45.2,
            ram_percent=60.1,
            active_tasks=2,
            status="HEALTHY",
        )
        # Verify no error raised

    def test_concurrent_writes(self):
        errors = []

        def worker_write(idx: int):
            try:
                for j in range(10):
                    self.storage.save_task(
                        task_id=f"concur_{idx}_{j}",
                        name="ConcurrentTask",
                        status="COMPLETED",
                        execution_time_ms=1.0,
                    )
                    self.storage.save_event("INFO", f"Thread {idx} event {j}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker_write, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(self.storage.count_tasks(), 40)


if __name__ == "__main__":
    unittest.main()
