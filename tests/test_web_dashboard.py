"""
Unit tests for MeshWeaver Web Dashboard and REST API.
"""

import json
import unittest
import urllib.request
import urllib.error
import time

from dashboard.cli import DashboardEventLogger
from dashboard.web_server import MeshWeaverWebServer
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import TaskRouter, WorkerNodeInfo


class TestMeshWeaverWebServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = 8089
        cls.host = "127.0.0.1"
        cls.monitor = GossipMonitor(local_node_id="test_gateway", host=cls.host, port=cls.port)
        cls.router = TaskRouter(local_node_id="test_gateway")
        cls.reliability = ReliabilityManager()
        cls.event_logger = DashboardEventLogger()

        # Add mock worker peer
        peer = cls.monitor.add_peer("test_worker_1", port=9001)
        peer.metrics.cpu_percent = 25.0
        peer.metrics.ram_percent = 40.0
        cls.router.register_or_update_worker(WorkerNodeInfo(
            node_id="test_worker_1",
            host=cls.host,
            port=9001,
            cpu_percent=25.0,
            ram_percent=40.0,
            is_alive=True,
        ))

        cls.server = MeshWeaverWebServer(
            host=cls.host,
            port=cls.port,
            monitor=cls.monitor,
            router=cls.router,
            reliability=cls.reliability,
            event_logger=cls.event_logger,
        )
        cls.server.start(background=True)
        time.sleep(0.2)  # Give server brief time to bind socket

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def _get(self, path: str):
        url = f"http://{self.host}:{self.port}{path}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            data = resp.read().decode("utf-8")
            return resp.status, resp.headers.get("Content-Type", ""), data

    def _post(self, path: str, payload: dict):
        url = f"http://{self.host}:{self.port}{path}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            data = resp.read().decode("utf-8")
            return resp.status, json.loads(data)

    def test_serve_index_html(self):
        status, content_type, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", content_type)
        self.assertIn("MeshWeaver", body)

    def test_api_status(self):
        status, _, body = self._get("/api/status")
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertIn("total_nodes", data)
        self.assertIn("healthy_nodes", data)
        self.assertGreaterEqual(data["total_nodes"], 2)

    def test_api_nodes(self):
        status, _, body = self._get("/api/nodes")
        self.assertEqual(status, 200)
        nodes = json.loads(body)
        self.assertIsInstance(nodes, list)
        node_ids = [n["node_id"] for n in nodes]
        self.assertIn("test_gateway", node_ids)
        self.assertIn("test_worker_1", node_ids)

    def test_api_events(self):
        self.event_logger.log("INFO", "Test event stream")
        status, _, body = self._get("/api/events")
        self.assertEqual(status, 200)
        events = json.loads(body)
        self.assertIsInstance(events, list)
        self.assertTrue(any("Test event stream" in e["message"] for e in events))

    def test_api_dispatch_task(self):
        status, res = self._post("/api/tasks", {
            "preset": "PrimeCount",
            "parameter": 500,
            "strategy": "LOWEST_CPU",
        })
        self.assertEqual(status, 200)
        self.assertEqual(res["status"], "COMPLETED")
        self.assertIn("execution_time_ms", res)
        self.assertEqual(res["result"], 95)  # 95 primes under 500

    def test_api_simulate_crash_and_recover(self):
        # Crash
        status, res = self._post("/api/simulate/crash", {"node_id": "test_worker_1"})
        self.assertEqual(status, 200)
        self.assertTrue(res["success"])
        peer = self.monitor.get_peer("test_worker_1")
        self.assertEqual(peer.health_state.value, "DEAD")

        # Recover
        status, res = self._post("/api/simulate/recover", {"node_id": "test_worker_1"})
        self.assertEqual(status, 200)
        self.assertTrue(res["success"])
        self.assertEqual(peer.health_state.value, "HEALTHY")


if __name__ == "__main__":
    unittest.main()
