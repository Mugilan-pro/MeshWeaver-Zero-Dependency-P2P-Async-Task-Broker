"""
Unit Tests for MeshWeaver Security Subsystem
Tests Ed25519 keypairs, signature generation and verification,
tamper detection, replay attack defense, and TLS certificate generation.
"""

import time
import unittest

from security.crypto import (
    NodeKeypair,
    SecurityError,
    SignatureVerificationError,
    ReplayAttackError,
)
from security.tls import (
    generate_self_signed_cert,
    create_server_ssl_context,
    create_client_ssl_context,
)


class TestSecurityCrypto(unittest.TestCase):
    def setUp(self):
        self.keypair = NodeKeypair()

    def test_keypair_generation_and_signing(self):
        self.assertIsNotNone(self.keypair.public_key_hex)
        self.assertEqual(len(self.keypair.public_key_hex), 64)  # 32 bytes = 64 hex chars

        message = b"MeshWeaver:Distributed:Task:Payload:12345"
        sig = self.keypair.sign(message)
        self.assertIsInstance(sig, str)
        self.assertEqual(len(sig), 128)  # 64 bytes = 128 hex chars for Ed25519

        is_valid = NodeKeypair.verify(message, sig, self.keypair.public_key_hex)
        self.assertTrue(is_valid)

    def test_invalid_signature_rejected(self):
        message = b"Valid Message"
        sig = self.keypair.sign(message)

        # Corrupted signature
        bad_sig = sig[:-4] + "0000"
        is_valid = NodeKeypair.verify(message, bad_sig, self.keypair.public_key_hex)
        self.assertFalse(is_valid)

        # Corrupted message
        is_valid_msg = NodeKeypair.verify(b"Different Message", sig, self.keypair.public_key_hex)
        self.assertFalse(is_valid_msg)

    def test_task_request_signing_and_verification(self):
        task_id = "task_001_abc"
        payload = b"compute_payload_bytes"
        origin = "node_alpha"

        auth = self.keypair.sign_task_request(
            task_id=task_id,
            payload_bytes=payload,
            origin_node_id=origin,
        )

        verifier_keypair = NodeKeypair()
        verified = verifier_keypair.verify_task_request(
            task_id=task_id,
            payload_bytes=payload,
            origin_node_id=origin,
            auth_metadata=auth,
        )
        self.assertTrue(verified)

    def test_tampered_task_payload_fails_verification(self):
        task_id = "task_002_tamper"
        payload = b"original_payload"
        origin = "node_alpha"

        auth = self.keypair.sign_task_request(
            task_id=task_id,
            payload_bytes=payload,
            origin_node_id=origin,
        )

        tampered_payload = b"altered_malicious_payload"
        verifier_keypair = NodeKeypair()

        with self.assertRaises(SignatureVerificationError):
            verifier_keypair.verify_task_request(
                task_id=task_id,
                payload_bytes=tampered_payload,
                origin_node_id=origin,
                auth_metadata=auth,
            )

    def test_replay_attack_prevention(self):
        task_id = "task_003_replay"
        payload = b"idempotent_payload"
        origin = "node_alpha"
        fixed_nonce = "fixed_nonce_12345"

        auth = self.keypair.sign_task_request(
            task_id=task_id,
            payload_bytes=payload,
            origin_node_id=origin,
            nonce=fixed_nonce,
        )

        verifier = NodeKeypair()
        # First verification succeeds
        self.assertTrue(verifier.verify_task_request(task_id, payload, origin, auth))

        # Replayed verification with exact same nonce must raise ReplayAttackError
        with self.assertRaises(ReplayAttackError):
            verifier.verify_task_request(task_id, payload, origin, auth)

    def test_expired_timestamp_fails(self):
        task_id = "task_004_expired"
        payload = b"old_payload"
        origin = "node_alpha"
        stale_timestamp = time.time() - 120.0  # 2 minutes ago

        auth = self.keypair.sign_task_request(
            task_id=task_id,
            payload_bytes=payload,
            origin_node_id=origin,
            timestamp=stale_timestamp,
        )

        verifier = NodeKeypair()
        with self.assertRaises(ReplayAttackError):
            verifier.verify_task_request(
                task_id=task_id,
                payload_bytes=payload,
                origin_node_id=origin,
                auth_metadata=auth,
                max_drift_seconds=30.0,
            )


class TestSecurityTLS(unittest.TestCase):
    def test_self_signed_cert_and_context_generation(self):
        cert_pem, key_pem = generate_self_signed_cert(common_name="test-mesh-node")
        self.assertIn(b"BEGIN CERTIFICATE", cert_pem)
        self.assertIn(b"BEGIN RSA PRIVATE KEY", key_pem)

        server_ctx = create_server_ssl_context(cert_pem, key_pem)
        self.assertIsNotNone(server_ctx)

        client_ctx = create_client_ssl_context(verify_peer=False)
        self.assertIsNotNone(client_ctx)
        self.assertFalse(client_ctx.check_hostname)


if __name__ == "__main__":
    unittest.main()
