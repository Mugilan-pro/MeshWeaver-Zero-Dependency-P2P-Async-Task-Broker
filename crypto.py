"""
MeshWeaver Cryptographic Signatures
Implements Ed25519 asymmetric keypairs and cryptographic signatures for task execution requests.
Every task request must be signed by the originating node's private key, and verified
by the receiving worker node before execution is permitted.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
import uuid
from typing import Dict, Optional, Set, Tuple

logger = logging.getLogger("MeshWeaver.Security.Crypto")

# Prefer cryptography library for Ed25519; fallback to HMAC-SHA256 if unavailable
try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from cryptography.hazmat.primitives import serialization
    from cryptography.exceptions import InvalidSignature
    _HAS_CRYPTOGRAPHY = True
except ImportError:
    _HAS_CRYPTOGRAPHY = False
    ed25519 = None  # type: ignore
    serialization = None  # type: ignore
    InvalidSignature = Exception  # type: ignore


class SecurityError(Exception):
    """Base exception for security and authentication failures."""
    pass


class SignatureVerificationError(SecurityError):
    """Raised when cryptographic signature verification fails (tampered or spoofed request)."""
    pass


class ReplayAttackError(SecurityError):
    """Raised when a task request contains an expired timestamp or reused nonce."""
    pass


class NodeKeypair:
    """
    Manages an asymmetric keypair (Ed25519 by default) for a MeshWeaver node.
    Enables signing task execution requests and verifying signatures from peers.
    """

    def __init__(self, private_key: Optional[Any] = None) -> None:
        if _HAS_CRYPTOGRAPHY:
            self._private_key = private_key or ed25519.Ed25519PrivateKey.generate()
            self._public_key = self._private_key.public_key()
            self._algo = "Ed25519"
        else:
            # Fallback symmetric/pseudo keypair for zero-dependency environments
            self._secret_seed = uuid.uuid4().bytes + uuid.uuid4().bytes
            self._algo = "HMAC-SHA256"

        # Seen nonces cache for replay protection: nonce -> expiry timestamp
        self._seen_nonces: Dict[str, float] = {}

    @property
    def algorithm(self) -> str:
        return self._algo

    @property
    def public_key_hex(self) -> str:
        """Returns the public key serialized as a lowercase hexadecimal string."""
        if _HAS_CRYPTOGRAPHY:
            raw_bytes = self._public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw,
            )
            return raw_bytes.hex()
        else:
            # Hash of the secret seed acts as public identity
            return hashlib.sha256(self._secret_seed).hexdigest()

    def sign(self, message: bytes) -> str:
        """
        Signs arbitrary bytes with the node's private key.
        Returns the signature as a lowercase hexadecimal string.
        """
        if _HAS_CRYPTOGRAPHY:
            signature_bytes = self._private_key.sign(message)
            return signature_bytes.hex()
        else:
            sig = hmac.new(self._secret_seed, message, hashlib.sha256).digest()
            return sig.hex()

    @staticmethod
    def verify(
        message: bytes,
        signature_hex: str,
        public_key_hex: str,
    ) -> bool:
        """
        Verifies a signature against a message and a peer's public key.
        Returns True if valid, False otherwise.
        """
        try:
            sig_bytes = bytes.fromhex(signature_hex)
            pub_bytes = bytes.fromhex(public_key_hex)
        except (ValueError, TypeError):
            return False

        if _HAS_CRYPTOGRAPHY:
            try:
                peer_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
                peer_pub_key.verify(sig_bytes, message)
                return True
            except InvalidSignature:
                return False
            except Exception as e:
                logger.debug(f"Signature check error: {e}")
                return False
        else:
            expected = hmac.new(pub_bytes, message, hashlib.sha256).digest()
            return hmac.compare_digest(sig_bytes, expected)

    @staticmethod
    def build_task_canonical_message(
        task_id: str,
        payload_bytes: bytes,
        origin_node_id: str,
        timestamp: float,
        nonce: str,
    ) -> bytes:
        """
        Constructs deterministic canonical bytes for signing and verification.
        Includes payload hash, task ID, origin node, timestamp, and nonce.
        """
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        canonical_str = f"MESHWEAVER:TASK:{task_id}:{origin_node_id}:{payload_hash}:{timestamp:.4f}:{nonce}"
        return canonical_str.encode("utf-8")

    def sign_task_request(
        self,
        task_id: str,
        payload_bytes: bytes,
        origin_node_id: str,
        timestamp: Optional[float] = None,
        nonce: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generates cryptographic authentication metadata for a task request.
        Returns a dict containing timestamp, nonce, public_key, and signature.
        """
        ts = timestamp if timestamp is not None else time.time()
        n = nonce or uuid.uuid4().hex
        canonical_msg = self.build_task_canonical_message(
            task_id=task_id,
            payload_bytes=payload_bytes,
            origin_node_id=origin_node_id,
            timestamp=ts,
            nonce=n,
        )
        signature = self.sign(canonical_msg)
        return {
            "signature": signature,
            "public_key": self.public_key_hex,
            "timestamp": ts,
            "nonce": n,
            "algorithm": self._algo,
        }

    def verify_task_request(
        self,
        task_id: str,
        payload_bytes: bytes,
        origin_node_id: str,
        auth_metadata: Dict[str, Any],
        max_drift_seconds: float = 60.0,
    ) -> bool:
        """
        Verifies that an incoming task request is authentic, untampered, and fresh.
        Enforces:
        1. Timestamp freshness bound (|now - ts| <= max_drift_seconds)
        2. Nonce uniqueness (prevents replay attacks)
        3. Ed25519 signature validity over the canonical message
        """
        signature = auth_metadata.get("signature")
        public_key = auth_metadata.get("public_key")
        timestamp = auth_metadata.get("timestamp")
        nonce = auth_metadata.get("nonce")

        if not signature or not public_key or timestamp is None or not nonce:
            raise SignatureVerificationError("Missing required cryptographic authentication fields")

        # 1. Freshness check
        now = time.time()
        if abs(now - float(timestamp)) > max_drift_seconds:
            raise ReplayAttackError(
                f"Task request timestamp expired or drifted beyond {max_drift_seconds}s "
                f"(drift: {abs(now - float(timestamp)):.2f}s)"
            )

        # 2. Nonce replay check
        self._purge_expired_nonces(now, max_drift_seconds)
        if nonce in self._seen_nonces:
            raise ReplayAttackError(f"Replay attack detected: nonce '{nonce}' was already used")
        self._seen_nonces[nonce] = now + max_drift_seconds

        # 3. Canonical message verification
        canonical_msg = self.build_task_canonical_message(
            task_id=task_id,
            payload_bytes=payload_bytes,
            origin_node_id=origin_node_id,
            timestamp=float(timestamp),
            nonce=nonce,
        )

        is_valid = self.verify(
            message=canonical_msg,
            signature_hex=signature,
            public_key_hex=public_key,
        )

        if not is_valid:
            raise SignatureVerificationError(
                f"Cryptographic signature check failed for task {task_id[:8]} "
                f"from origin node {origin_node_id}"
            )

        return True

    def _purge_expired_nonces(self, current_time: float, max_drift_seconds: float) -> None:
        """Removes expired nonces from memory to prevent memory leaks."""
        expired = [n for n, exp in self._seen_nonces.items() if exp < current_time]
        for n in expired:
            del self._seen_nonces[n]
