"""
Cryptographic signing and verification for task requests and results.
Uses HMAC-SHA256 for integrity authentication and anti-replay window checks.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, Optional, Tuple

from config import DEFAULT_CLUSTER_SECRET, SIGNATURE_MAX_AGE_SECONDS
from task.models import Task, TaskResult

logger = logging.getLogger("MeshWeaver.Security")


class SecurityError(Exception):
    """Base class for all security-related exceptions."""
    pass


class SignatureVerificationError(SecurityError):
    """Raised when cryptographic signature is invalid or payload has been tampered with."""
    pass


class ReplayAttackError(SecurityError):
    """Raised when a signature's timestamp is stale or outside the allowed clock skew window."""
    pass


def compute_payload_digest(payload: bytes) -> str:
    """Computes SHA-256 hex digest of raw binary bytes."""
    return hashlib.sha256(payload).hexdigest()


def build_canonical_task_string(
    task_id: str,
    name: str,
    payload_hash: str,
    signer_node_id: str,
    timestamp: float,
) -> str:
    """
    Constructs a deterministic canonical string representation for cryptographic signing.
    Format: MESHWEAVER_TASK_V1:{task_id}:{name}:{payload_hash}:{signer_node_id}:{timestamp:.4f}
    """
    return f"MESHWEAVER_TASK_V1:{task_id}:{name}:{payload_hash}:{signer_node_id}:{timestamp:.4f}"


def build_canonical_result_string(
    task_id: str,
    status: str,
    payload_hash: str,
    signer_node_id: str,
    timestamp: float,
) -> str:
    """
    Constructs a deterministic canonical string representation for task execution results.
    Format: MESHWEAVER_RESULT_V1:{task_id}:{status}:{payload_hash}:{signer_node_id}:{timestamp:.4f}
    """
    return f"MESHWEAVER_RESULT_V1:{task_id}:{status}:{payload_hash}:{signer_node_id}:{timestamp:.4f}"


def sign_task_request(
    task: Task,
    secret_key: bytes,
    signer_node_id: str,
    timestamp: Optional[float] = None,
) -> Task:
    """
    Cryptographically signs a Task instance using HMAC-SHA256.
    Populates signature, signer_node_id, and signature_timestamp fields in-place.
    """
    ts = timestamp if timestamp is not None else time.time()
    payload_hash = compute_payload_digest(task.payload)
    canonical = build_canonical_task_string(
        task_id=task.task_id,
        name=task.name,
        payload_hash=payload_hash,
        signer_node_id=signer_node_id,
        timestamp=ts,
    )
    sig = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    task.signature = sig
    task.signer_node_id = signer_node_id
    task.signature_timestamp = ts
    return task


def verify_task_request(
    task: Task,
    secret_key: bytes,
    max_age_seconds: float = SIGNATURE_MAX_AGE_SECONDS,
    current_time: Optional[float] = None,
) -> bool:
    """
    Validates the cryptographic signature and timestamp freshness of a Task instance.
    Raises:
        SignatureVerificationError: If signature is missing, invalid, or payload tampered.
        ReplayAttackError: If timestamp is stale (> max_age_seconds) or from the future.
    """
    if not task.signature:
        raise SignatureVerificationError(f"Task {task.task_id[:8]} lacks a cryptographic signature")
    if not task.signer_node_id:
        raise SignatureVerificationError(f"Task {task.task_id[:8]} lacks a signer node identity")
    if task.signature_timestamp is None:
        raise SignatureVerificationError(f"Task {task.task_id[:8]} lacks a signature timestamp")

    now = current_time if current_time is not None else time.time()
    age = now - task.signature_timestamp

    # Clock skew & replay protection: allow up to max_age_seconds in past, 5s in future
    if age < -5.0 or age > max_age_seconds:
        raise ReplayAttackError(
            f"Task {task.task_id[:8]} signature expired or clock desynchronized "
            f"(age={age:.2f}s, max_allowed={max_age_seconds}s)"
        )

    payload_hash = compute_payload_digest(task.payload)
    canonical = build_canonical_task_string(
        task_id=task.task_id,
        name=task.name,
        payload_hash=payload_hash,
        signer_node_id=task.signer_node_id,
        timestamp=task.signature_timestamp,
    )
    expected_sig = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(task.signature, expected_sig):
        raise SignatureVerificationError(
            f"Cryptographic signature mismatch on task {task.task_id[:8]}. "
            f"Payload or header may have been modified or tampered with in transit."
        )

    return True


def sign_task_result(
    result: TaskResult,
    secret_key: bytes,
    signer_node_id: str,
    timestamp: Optional[float] = None,
) -> TaskResult:
    """Cryptographically signs a TaskResult using HMAC-SHA256."""
    ts = timestamp if timestamp is not None else time.time()
    payload_bytes = result.result_payload or b""
    payload_hash = compute_payload_digest(payload_bytes)
    status_str = result.status.value if hasattr(result.status, "value") else str(result.status)
    canonical = build_canonical_result_string(
        task_id=result.task_id,
        status=status_str,
        payload_hash=payload_hash,
        signer_node_id=signer_node_id,
        timestamp=ts,
    )
    sig = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    result.signature = sig
    result.signer_node_id = signer_node_id
    result.timestamp = ts
    return result


def verify_task_result(
    result: TaskResult,
    secret_key: bytes,
    max_age_seconds: float = SIGNATURE_MAX_AGE_SECONDS,
    current_time: Optional[float] = None,
) -> bool:
    """Validates the cryptographic signature of an execution result."""
    if not result.signature or not result.signer_node_id:
        raise SignatureVerificationError(f"TaskResult {result.task_id[:8]} lacks cryptographic signature")

    now = current_time if current_time is not None else time.time()
    age = now - result.timestamp
    if age < -5.0 or age > max_age_seconds:
        raise ReplayAttackError(
            f"TaskResult {result.task_id[:8]} signature expired (age={age:.2f}s, max_allowed={max_age_seconds}s)"
        )

    payload_bytes = result.result_payload or b""
    payload_hash = compute_payload_digest(payload_bytes)
    status_str = result.status.value if hasattr(result.status, "value") else str(result.status)
    canonical = build_canonical_result_string(
        task_id=result.task_id,
        status=status_str,
        payload_hash=payload_hash,
        signer_node_id=result.signer_node_id,
        timestamp=result.timestamp,
    )
    expected_sig = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(result.signature, expected_sig):
        raise SignatureVerificationError(
            f"Cryptographic signature mismatch on result for task {result.task_id[:8]}"
        )

    return True


class SecurityManager:
    """
    Subsystem managing node cryptographic keys, signing outgoing requests,
    and enforcing cryptographic verification on all inbound task execution workloads.
    """

    def __init__(
        self,
        node_id: str,
        secret_key: bytes = DEFAULT_CLUSTER_SECRET,
        require_signatures: bool = True,
        max_age_seconds: float = SIGNATURE_MAX_AGE_SECONDS,
    ) -> None:
        self.node_id = node_id
        self.secret_key = secret_key
        self.require_signatures = require_signatures
        self.max_age_seconds = max_age_seconds

        self.stats = {
            "signatures_created": 0,
            "signatures_verified": 0,
            "tamper_rejections": 0,
            "replay_rejections": 0,
        }

    def sign_task(self, task: Task) -> Task:
        """Signs an outgoing task."""
        signed = sign_task_request(task, self.secret_key, self.node_id)
        self.stats["signatures_created"] += 1
        return signed

    def verify_task(self, task: Task) -> bool:
        """Verifies an incoming task execution request."""
        if not self.require_signatures:
            return True

        try:
            verified = verify_task_request(task, self.secret_key, self.max_age_seconds)
            if verified:
                self.stats["signatures_verified"] += 1
            return verified
        except ReplayAttackError as e:
            self.stats["replay_rejections"] += 1
            logger.warning(f"[{self.node_id}] REPLAY ATTACK BLOCKED: {e}")
            raise
        except SignatureVerificationError as e:
            self.stats["tamper_rejections"] += 1
            logger.error(f"[{self.node_id}] SECURITY ALERT: Tampered task rejected: {e}")
            raise

    def sign_result(self, result: TaskResult) -> TaskResult:
        """Signs an outgoing task result."""
        signed = sign_task_result(result, self.secret_key, self.node_id)
        self.stats["signatures_created"] += 1
        return signed

    def verify_result(self, result: TaskResult) -> bool:
        """Verifies an incoming execution result."""
        if not self.require_signatures:
            return True

        try:
            verified = verify_task_result(result, self.secret_key, self.max_age_seconds)
            if verified:
                self.stats["signatures_verified"] += 1
            return verified
        except (ReplayAttackError, SignatureVerificationError) as e:
            self.stats["tamper_rejections"] += 1
            logger.error(f"[{self.node_id}] SECURITY ALERT: Invalid result signature: {e}")
            raise
