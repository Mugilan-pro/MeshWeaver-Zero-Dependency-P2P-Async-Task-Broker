"""
Tests for MeshWeaver Task Security & Cryptographic Verification Engine
Validates task signing, tamper detection, untrusted sender rejection,
wire format preservation, and TaskExecutor security enforcement.
Compatible with Python 3.7+ through 3.13+.
"""

import asyncio
import unittest

from task.models import Task, TaskStatus
from task.serializer import TaskSerializer
from task.executor import TaskExecutor
from task.security import (
    TaskSigner,
    TaskVerifier,
    SecurityError,
    MissingSignatureError,
    UntrustedNodeError,
    InvalidSignatureError,
    sign_task,
)


def sample_multiply(a: int, b: int) -> int:
    return a * b


class TestTaskSecurity(unittest.TestCase):
    def setUp(self):
        self.key_alice = b"secret_key_alice_12345"
        self.key_bob = b"secret_key_bob_67890"

        self.signer_alice = TaskSigner(node_id="node_alice", secret_key=self.key_alice)
        self.verifier = TaskVerifier(
            trusted_keys={
                "node_alice": self.key_alice,
                "node_bob": self.key_bob,
            },
            enforce_signatures=True,
        )

    def test_sign_and_verify_valid_task(self):
        task = Task(name="test_workload", payload=b"\x01\x02\x03", origin_node_id="node_alice")
        sig = self.signer_alice.sign(task)

        self.assertIsNotNone(task.signature)
        self.assertEqual(task.signer_node_id, "node_alice")
        self.assertEqual(sig, task.signature)

        # Verification must pass
        self.assertTrue(self.verifier.verify(task))

    def test_tampered_payload_rejected(self):
        task = Task(name="test_workload", payload=b"original_payload", origin_node_id="node_alice")
        self.signer_alice.sign(task)

        # Attacker tampers with 1 byte in payload
        task.payload = b"modified_payload"

        with self.assertRaises(InvalidSignatureError):
            self.verifier.verify(task)

    def test_tampered_task_id_rejected(self):
        task = Task(name="test_workload", payload=b"payload_bytes", origin_node_id="node_alice")
        self.signer_alice.sign(task)

        # Attacker swaps task_id
        task.task_id = "forged_task_id_9999"

        with self.assertRaises(InvalidSignatureError):
            self.verifier.verify(task)

    def test_missing_signature_rejected(self):
        task = Task(name="unsigned_workload", payload=b"payload", origin_node_id="node_alice")

        with self.assertRaises(MissingSignatureError):
            self.verifier.verify(task)

    def test_untrusted_signer_node_rejected(self):
        attacker_key = b"evil_attacker_key"
        attacker_signer = TaskSigner(node_id="rogue_node_x", secret_key=attacker_key)
        task = Task(name="malicious_workload", payload=b"drop_db", origin_node_id="rogue_node_x")
        attacker_signer.sign(task)

        with self.assertRaises(UntrustedNodeError):
            self.verifier.verify(task)

    def test_key_revocation(self):
        task = Task(name="test_workload", payload=b"data", origin_node_id="node_bob")
        signer_bob = TaskSigner("node_bob", self.key_bob)
        signer_bob.sign(task)

        # Verified before revocation
        self.assertTrue(self.verifier.verify(task))

        # Revoke node_bob
        self.verifier.revoke_trusted_node("node_bob")

        with self.assertRaises(UntrustedNodeError):
            self.verifier.verify(task)

    def test_wire_serialization_preserves_signature(self):
        task = Task(name="wire_job", payload=b"\xaa\xbb\xcc", origin_node_id="node_alice")
        self.signer_alice.sign(task)

        # Serialize to dict and restore (simulating network transmission)
        wire_dict = task.to_dict()
        restored_task = Task.from_dict(wire_dict)

        self.assertEqual(restored_task.signature, task.signature)
        self.assertEqual(restored_task.signer_node_id, "node_alice")
        self.assertTrue(self.verifier.verify(restored_task))


class TestExecutorSecurityIntegration(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.key = b"mesh_cluster_key_alpha"
        self.verifier = TaskVerifier(trusted_keys={"node_trusted": self.key}, enforce_signatures=True)
        self.executor = TaskExecutor(node_id="worker_secured", max_workers=2, verifier=self.verifier)

    def tearDown(self):
        self.executor.shutdown(wait=True)
        self.loop.close()

    def test_executor_rejects_unsigned_task(self):
        raw_task = TaskSerializer.package_task("unsigned_calc", sample_multiply, args=(6, 7))

        res = self.loop.run_until_complete(self.executor.execute(raw_task))
        self.assertEqual(res.status, TaskStatus.FAILED)
        self.assertIn("Security verification rejected", res.error_message)

    def test_executor_accepts_valid_signed_task(self):
        task = TaskSerializer.package_task("signed_calc", sample_multiply, args=(6, 7), origin_node_id="node_trusted")
        sign_task(task, "node_trusted", self.key)

        res = self.loop.run_until_complete(self.executor.execute(task))
        self.assertEqual(res.status, TaskStatus.COMPLETED)
        self.assertEqual(res.return_value, 42)


if __name__ == "__main__":
    unittest.main()
