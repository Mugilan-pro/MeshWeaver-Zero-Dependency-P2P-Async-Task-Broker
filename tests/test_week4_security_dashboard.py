"""
MeshWeaver Week 4 Test Suite: TLS/SSL Security, Cryptographic Signatures & Live Dashboard
Validates:
1. TLS/SSL transport wrapping & X.509 certificate generation.
2. In-memory TLS encryption/decryption channel (OpenSSL/LibreSSL).
3. Cryptographic signing of task execution requests (HMAC-SHA256).
4. Tampering detection and rejection of forged tasks.
5. Anti-replay protection with timestamp expiration.
6. Execution result signing and verification.
7. Rich/ANSI Terminal Dashboard: Real-time mesh topology rendering.
8. Rich/ANSI Terminal Dashboard: Live task execution state & DLQ inspection.
"""

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import DashboardEventLogger, RichTerminalDashboard, _render_bar
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import TaskRouter
from security.crypto import (
    ReplayAttackError,
    SecurityManager,
    SignatureVerificationError,
    sign_task_request,
    sign_task_result,
    verify_task_request,
    verify_task_result,
)
from security.tls import (
    TLSMemoryChannel,
    create_client_ssl_context,
    create_server_ssl_context,
    ensure_node_certificates,
    generate_self_signed_cert,
)
from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.serializer import TaskSerializer


class TestWeek4TLSSecurity(unittest.TestCase):
    """Verifies transport encryption and SSL context initialization (Week 4 Milestone)."""

    def test_x509_certificate_generation(self):
        """Generates self-signed certificate and RSA 2048-bit key pair."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "test_node.crt")
            key_path = os.path.join(tmpdir, "test_node.key")

            res_cert, res_key = generate_self_signed_cert(cert_path, key_path, common_name="node_test_01")
            self.assertTrue(os.path.exists(res_cert))
            self.assertTrue(os.path.exists(res_key))
            self.assertGreater(os.path.getsize(res_cert), 100)
            self.assertGreater(os.path.getsize(res_key), 100)

    def test_server_and_client_ssl_contexts(self):
        """Initializes server and client SSLContexts with modern TLS protocols."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "ssl_test.crt")
            key_path = os.path.join(tmpdir, "ssl_test.key")
            generate_self_signed_cert(cert_path, key_path, common_name="127.0.0.1")

            server_ctx = create_server_ssl_context(cert_path, key_path)
            self.assertIsNotNone(server_ctx)

            client_ctx = create_client_ssl_context(verify_peer=False)
            self.assertIsNotNone(client_ctx)

    def test_in_memory_tls_handshake_and_encryption(self):
        """Executes full TLS handshake, cipher negotiation, and bidirectional encryption in memory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cert_path = os.path.join(tmpdir, "tls_mem.crt")
            key_path = os.path.join(tmpdir, "tls_mem.key")
            generate_self_signed_cert(cert_path, key_path, common_name="127.0.0.1")

            server_ctx = create_server_ssl_context(cert_path, key_path)
            client_ctx = create_client_ssl_context(verify_peer=False)

            channel = TLSMemoryChannel(server_ctx, client_ctx)
            channel.perform_handshake()

            cipher_info = channel.cipher
            self.assertIsNotNone(cipher_info)
            # Cipher tuple: (name, protocol, bits)
            self.assertIn("TLS", cipher_info[1])
            self.assertGreaterEqual(cipher_info[2], 128)

            # Test client -> server encryption
            secret_payload = b"CRITICAL_DISTRIBUTED_COMPUTE_TASK_DATA"
            wire_ciphertext = channel.client_send(secret_payload)
            self.assertNotEqual(wire_ciphertext, secret_payload)

            decrypted_server = channel.server_receive(wire_ciphertext)
            self.assertEqual(decrypted_server, secret_payload)

            # Test server -> client encrypted response
            response_payload = b"COMPUTATION_RESULT_42"
            resp_ciphertext = channel.server_send(response_payload)
            decrypted_client = channel.client_receive(resp_ciphertext)
            self.assertEqual(decrypted_client, response_payload)


class TestWeek4CryptographicSignatures(unittest.TestCase):
    """Verifies cryptographic request signing, tamper rejection, and anti-replay (Week 4 Milestone)."""

    def setUp(self):
        self.secret_key = b"super-secure-cluster-secret-key-32b"
        self.security = SecurityManager(node_id="origin_node", secret_key=self.secret_key)

    def test_sign_and_verify_valid_task(self):
        """Valid task signature passes verification."""
        task = Task(name="benchmark_prime", payload=b"sieve_parameters", origin_node_id="origin_node")
        self.security.sign_task(task)

        self.assertIsNotNone(task.signature)
        self.assertEqual(task.signer_node_id, "origin_node")
        self.assertIsNotNone(task.signature_timestamp)

        # Verification succeeds
        verified = self.security.verify_task(task)
        self.assertTrue(verified)
        self.assertEqual(self.security.stats["signatures_verified"], 1)

    def test_tamper_detection_on_payload(self):
        """Modified binary payload triggers SignatureVerificationError."""
        task = Task(name="matrix_multiply", payload=b"original_matrices", origin_node_id="origin_node")
        self.security.sign_task(task)

        # Adversary mutates task payload in transit
        task.payload = b"malicious_tampered_payload"

        with self.assertRaises(SignatureVerificationError):
            self.security.verify_task(task)
        self.assertEqual(self.security.stats["tamper_rejections"], 1)

    def test_tamper_detection_on_task_id(self):
        """Altered task_id header triggers SignatureVerificationError."""
        task = Task(name="job", payload=b"data", origin_node_id="origin_node")
        self.security.sign_task(task)

        task.task_id = "forged_task_id_9999"

        with self.assertRaises(SignatureVerificationError):
            self.security.verify_task(task)

    def test_replay_attack_rejection(self):
        """Expired signature timestamp triggers ReplayAttackError."""
        task = Task(name="stale_job", payload=b"data", origin_node_id="origin_node")
        # Sign with timestamp 120 seconds in the past (> 60s max age)
        sign_task_request(task, self.secret_key, "origin_node", timestamp=time.time() - 120.0)

        with self.assertRaises(ReplayAttackError):
            self.security.verify_task(task)
        self.assertEqual(self.security.stats["replay_rejections"], 1)

    def test_sign_and_verify_task_result(self):
        """Execution results returned by workers are also cryptographically signed."""
        result = TaskResult.success(
            task_id="abc12345",
            return_value=100,
            result_payload=b"serialized_100",
            executed_by_node_id="worker_node",
        )
        self.security.sign_result(result)
        self.assertIsNotNone(result.signature)

        verified = self.security.verify_result(result)
        self.assertTrue(verified)

        # Tampered result payload triggers rejection
        result.result_payload = b"tampered_result"
        with self.assertRaises(SignatureVerificationError):
            self.security.verify_result(result)


class TestWeek4DashboardRefineAndPolish(unittest.TestCase):
    """Verifies live CLI dashboard showing real-time mesh topology and task execution state."""

    def setUp(self):
        self.monitor = GossipMonitor(local_node_id="dash_hub_01", host="127.0.0.1", port=9000)
        self.router = TaskRouter(local_node_id="dash_hub_01")
        self.reliability = ReliabilityManager()

        p1 = self.monitor.add_peer("peer_worker_a", host="127.0.0.1", port=9001)
        p1.metrics.cpu_percent = 25.0
        p1.metrics.ram_percent = 40.0
        p1.round_trip_time_ms = 1.2
        p1.is_tls = True

        p2 = self.monitor.add_peer("peer_worker_b", host="127.0.0.1", port=9002)
        p2.metrics.cpu_percent = 85.0
        p2.metrics.ram_percent = 70.0
        p2.round_trip_time_ms = 3.5
        p2.is_tls = True

        self.event_logger = DashboardEventLogger()
        self.dashboard = RichTerminalDashboard(
            monitor=self.monitor,
            router=self.router,
            reliability=self.reliability,
            event_logger=self.event_logger,
        )

    def test_dashboard_renders_mesh_topology(self):
        """Dashboard snapshot must include real-time mesh topology section with latency and TLS."""
        snapshot = self.dashboard.render_snapshot()
        self.assertIn("Mesh Topology", snapshot)
        self.assertIn("peer_worker_a", snapshot)
        self.assertIn("peer_worker_b", snapshot)
        self.assertIn("Latency:", snapshot)
        self.assertIn("TLS", snapshot)

    def test_dashboard_renders_task_execution_state(self):
        """Dashboard snapshot must display active in-flight task leases and DLQ."""
        task = Task(name="monte_carlo_pi", payload=b"pi", origin_node_id="dash_hub_01")
        task.signature = "sig_hmac_12345"
        self.reliability.assign_task(task, "peer_worker_a")

        snapshot = self.dashboard.render_snapshot()
        self.assertIn("Live Task Execution", snapshot)
        self.assertIn("monte_carlo_pi", snapshot)
        self.assertIn("peer_worker_a", snapshot)
        self.assertIn("RUNNING", snapshot)
        self.assertIn("Signed", snapshot)

    def test_dashboard_renders_dead_letter_queue_alert(self):
        """Failed tasks in DLQ appear in the dashboard snapshot."""
        task = Task(name="unrecoverable_job", payload=b"", max_retries=1)
        self.reliability.assign_task(task, "peer_worker_a")
        self.reliability.record_failure_and_retry(task.task_id, "Fatal Hardware Exception", self.router)
        self.reliability.record_failure_and_retry(task.task_id, "Fatal Hardware Exception", self.router)

        self.assertEqual(self.reliability.dead_letter_count, 1)
        snapshot = self.dashboard.render_snapshot()
        self.assertIn("DEAD-LETTER", snapshot)
        self.assertIn("unrecoverable_job", snapshot)


if __name__ == "__main__":
    unittest.main()
