"""
MeshWeaver Task Security & Cryptographic Verification Engine
Provides cryptographic signing, tamper detection, and identity verification
for distributed task execution requests across the peer-to-peer mesh.

Uses HMAC-SHA256 with constant-time digest comparison to prevent forgery,
man-in-the-middle tampering, and timing attacks.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Dict, Optional

from .models import Task

logger = logging.getLogger("MeshWeaver.TaskSecurity")


class SecurityError(Exception):
    """Base exception for all MeshWeaver security violations."""
    pass


class MissingSignatureError(SecurityError):
    """Raised when a task request lacks a required cryptographic signature."""
    pass


class UntrustedNodeError(SecurityError):
    """Raised when a task is signed by a node not recognized in the trusted keyring."""
    pass


class InvalidSignatureError(SecurityError):
    """Raised when signature verification fails, indicating payload tampering or key mismatch."""
    pass


def compute_canonical_task_bytes(task: Task) -> bytes:
    """
    Computes a deterministic byte representation of a task for cryptographic signing.
    Binds the task ID, name, origin node, and raw payload together.
    """
    header = f"{task.task_id}:{task.name}:{task.origin_node_id or ''}:{len(task.payload)}:".encode("utf-8")
    return header + task.payload


class TaskSigner:
    """
    Cryptographically signs task execution requests using a node's private or shared key.
    """

    def __init__(self, node_id: str, secret_key: bytes) -> None:
        self.node_id = node_id
        self.secret_key = secret_key

    def sign(self, task: Task) -> str:
        """
        Signs the given task, attaching the signature and signer identity directly to the model.
        Returns the hexadecimal signature string.
        """
        canonical_bytes = compute_canonical_task_bytes(task)
        signature = hmac.new(self.secret_key, canonical_bytes, hashlib.sha256).hexdigest()

        task.signature = signature
        task.signer_node_id = self.node_id
        logger.debug(f"Task {task.task_id[:8]} signed by {self.node_id} (sig: {signature[:12]}...)")
        return signature


class TaskVerifier:
    """
    Validates cryptographic signatures on incoming task execution requests.
    Enforces authorization, detects payload tampering, and blocks forged tasks.
    """

    def __init__(
        self,
        trusted_keys: Optional[Dict[str, bytes]] = None,
        shared_mesh_key: Optional[bytes] = None,
        enforce_signatures: bool = True,
    ) -> None:
        self.trusted_keys: Dict[str, bytes] = dict(trusted_keys) if trusted_keys else {}
        self.shared_mesh_key = shared_mesh_key
        self.enforce_signatures = enforce_signatures

    def register_trusted_node(self, node_id: str, secret_key: bytes) -> None:
        """Registers a trusted node ID and its corresponding secret key."""
        self.trusted_keys[node_id] = secret_key
        logger.info(f"Registered trusted security credentials for node {node_id}")

    def revoke_trusted_node(self, node_id: str) -> None:
        """Revokes trust for a compromised or decommissioned node."""
        self.trusted_keys.pop(node_id, None)
        logger.warning(f"Revoked trust for node {node_id}")

    def verify(self, task: Task) -> bool:
        """
        Verifies the cryptographic integrity and authenticity of a task.
        Raises a SecurityError subclass if verification fails.
        """
        if not self.enforce_signatures:
            return True

        if not task.signature:
            raise MissingSignatureError(f"Task {task.task_id[:8]} lacks a required cryptographic signature")

        signer_id = task.signer_node_id
        if not signer_id:
            raise UntrustedNodeError(f"Task {task.task_id[:8]} has a signature but no signer_node_id specified")

        # Resolve signing key (node-specific key takes precedence over shared mesh key)
        signing_key = self.trusted_keys.get(signer_id) or self.shared_mesh_key
        if not signing_key:
            raise UntrustedNodeError(f"Signer node '{signer_id}' is not in the trusted keyring")

        canonical_bytes = compute_canonical_task_bytes(task)
        expected_signature = hmac.new(signing_key, canonical_bytes, hashlib.sha256).hexdigest()

        # Constant-time comparison to prevent side-channel timing attacks
        if not hmac.compare_digest(task.signature, expected_signature):
            raise InvalidSignatureError(
                f"Cryptographic signature mismatch for task {task.task_id[:8]}! "
                "The payload was tampered with in transit or the key is invalid."
            )

        logger.debug(f"Task {task.task_id[:8]} signature verified successfully from {signer_id}")
        return True


def sign_task(task: Task, signer_node_id: str, secret_key: bytes) -> Task:
    """Convenience helper to sign a task in one call."""
    signer = TaskSigner(signer_node_id, secret_key)
    signer.sign(task)
    return task


def verify_task(task: Task, secret_key: bytes) -> bool:
    """Convenience helper to verify a task using a single secret key."""
    verifier = TaskVerifier(shared_mesh_key=secret_key, enforce_signatures=True)
    return verifier.verify(task)
