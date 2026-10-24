"""
Node ID and XOR distance metric for MeshWeaver DHT.
Follows standard Kademlia design (160-bit keyspace, XOR distance, prefix matching).
"""

from __future__ import annotations

import hashlib
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, Union

# Standard 160-bit Kademlia keyspace (20 bytes, matching SHA-1)
DEFAULT_ID_BITS = 160


class NodeID:
    """
    Represents a 160-bit node identifier in the Kademlia DHT.
    Internally stored as an integer so XOR distance operations are fast.
    """

    __slots__ = ("_value", "_num_bits")

    def __init__(self, value: Union[int, bytes, str], num_bits: int = DEFAULT_ID_BITS) -> None:
        self._num_bits = num_bits
        max_val = (1 << num_bits) - 1

        if isinstance(value, int):
            if value < 0 or value > max_val:
                raise ValueError(f"Value {value} out of range [0, 2^{num_bits}-1]")
            self._value = value
        elif isinstance(value, bytes):
            expected_len = (num_bits + 7) // 8
            if len(value) != expected_len:
                # If raw bytes don't match expected length, hash with SHA-1
                value = hashlib.sha1(value).digest()
            self._value = int.from_bytes(value, byteorder="big")
        elif isinstance(value, str):
            clean_str = value.strip().lower()
            expected_hex_len = (num_bits + 3) // 4
            # Check if it's already a valid hex string of expected length
            if len(clean_str) == expected_hex_len and all(c in "0123456789abcdef" for c in clean_str):
                self._value = int(clean_str, 16)
            else:
                # Hash arbitrary string to 160 bits using SHA-1
                digest = hashlib.sha1(clean_str.encode("utf-8")).digest()
                self._value = int.from_bytes(digest, byteorder="big")
        else:
            raise TypeError(f"Cannot create NodeID from type: {type(value)}")

    @classmethod
    def generate(cls, num_bits: int = DEFAULT_ID_BITS) -> NodeID:
        """Create a random NodeID using secure random bytes."""
        byte_count = (num_bits + 7) // 8
        return cls(secrets.token_bytes(byte_count), num_bits=num_bits)

    @classmethod
    def from_str(cls, text: str, num_bits: int = DEFAULT_ID_BITS) -> NodeID:
        """Hash a string (like a hostname or name) into a deterministic NodeID."""
        return cls(text, num_bits=num_bits)

    @classmethod
    def from_endpoint(cls, host: str, port: int, num_bits: int = DEFAULT_ID_BITS) -> NodeID:
        """Create a deterministic NodeID from an IP/port pair."""
        return cls(f"{host}:{port}", num_bits=num_bits)

    @classmethod
    def from_hex(cls, hex_str: str, num_bits: int = DEFAULT_ID_BITS) -> NodeID:
        return cls(hex_str, num_bits=num_bits)

    @classmethod
    def from_int(cls, val: int, num_bits: int = DEFAULT_ID_BITS) -> NodeID:
        return cls(val, num_bits=num_bits)

    @property
    def int_value(self) -> int:
        return self._value

    @property
    def bytes_value(self) -> bytes:
        byte_count = (self._num_bits + 7) // 8
        return self._value.to_bytes(byte_count, byteorder="big")

    @property
    def hex_value(self) -> str:
        hex_len = (self._num_bits + 3) // 4
        return f"{self._value:0{hex_len}x}"

    @property
    def num_bits(self) -> int:
        return self._num_bits

    def short_hex(self, length: int = 8) -> str:
        """Shortened hex string for log messages and printing."""
        return f"{self.hex_value[:length]}..."

    def xor_distance(self, other: NodeID) -> int:
        """
        Calculates XOR distance metric: d(x, y) = x ^ y.
        Returns distance as an integer.
        """
        if not isinstance(other, NodeID):
            raise TypeError(f"Expected NodeID, got {type(other)}")
        return self._value ^ other._value

    def shared_prefix_length(self, other: NodeID) -> int:
        """
        Count how many leading bits match between two IDs.
        Returns value between 0 (differ at MSB) and num_bits (identical).
        """
        dist = self.xor_distance(other)
        if dist == 0:
            return self._num_bits
        return self._num_bits - dist.bit_length()

    def bucket_index(self, other: NodeID) -> int:
        """
        Finds which k-bucket [0, num_bits-1] another node belongs to.
        Bucket i contains nodes at distance [2^i, 2^(i+1) - 1].
        """
        dist = self.xor_distance(other)
        if dist == 0:
            raise ValueError("A node cannot be placed into its own bucket (distance is 0)")
        return dist.bit_length() - 1

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return self._value == other._value and self._num_bits == other._num_bits
        return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __lt__(self, other: NodeID) -> bool:
        if not isinstance(other, NodeID):
            return NotImplemented
        return self._value < other._value

    def __le__(self, other: NodeID) -> bool:
        if not isinstance(other, NodeID):
            return NotImplemented
        return self._value <= other._value

    def __gt__(self, other: NodeID) -> bool:
        if not isinstance(other, NodeID):
            return NotImplemented
        return self._value > other._value

    def __ge__(self, other: NodeID) -> bool:
        if not isinstance(other, NodeID):
            return NotImplemented
        return self._value >= other._value

    def __hash__(self) -> int:
        return hash((self._value, self._num_bits))

    def __str__(self) -> str:
        return self.hex_value

    def __repr__(self) -> str:
        return f"NodeID({self.short_hex(8)})"


def xor_distance(id1: NodeID, id2: NodeID) -> int:
    """Helper to compute XOR distance between two NodeIDs."""
    return id1.xor_distance(id2)


def shared_prefix_length(id1: NodeID, id2: NodeID) -> int:
    """Helper to compute shared prefix bit length."""
    return id1.shared_prefix_length(id2)


@dataclass
class NodeContact:
    """
    Information about a known peer in the DHT.
    Stores the node's ID, network endpoint, and last seen timestamp.
    """
    node_id: NodeID
    host: str = "127.0.0.1"
    port: int = 8000
    last_seen: float = field(default_factory=time.time)
    rtt_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def endpoint(self) -> Tuple[str, int]:
        return self.host, self.port

    @property
    def endpoint_str(self) -> str:
        return f"{self.host}:{self.port}"

    def touch(self, rtt_ms: Optional[float] = None) -> None:
        """Mark contact as active right now."""
        self.last_seen = time.time()
        if rtt_ms is not None:
            self.rtt_ms = max(0.0, float(rtt_ms))

    def is_stale(self, timeout_seconds: float = 60.0) -> bool:
        """Check if we haven't heard from this peer recently."""
        return (time.time() - self.last_seen) > timeout_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert contact to a dictionary for JSON over the wire."""
        return {
            "node_id": self.node_id.hex_value,
            "host": self.host,
            "port": self.port,
            "last_seen": self.last_seen,
            "rtt_ms": self.rtt_ms,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> NodeContact:
        node_id_val = data["node_id"]
        node_id = NodeID.from_hex(node_id_val) if isinstance(node_id_val, str) else node_id_val
        return cls(
            node_id=node_id,
            host=str(data.get("host", "127.0.0.1")),
            port=int(data.get("port", 8000)),
            last_seen=float(data.get("last_seen", time.time())),
            rtt_ms=float(data.get("rtt_ms", 0.0)),
            metadata=dict(data.get("metadata", {})),
        )

    def __hash__(self) -> int:
        return hash(self.node_id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeContact):
            return self.node_id == other.node_id
        return False

    def __repr__(self) -> str:
        return f"NodeContact({self.node_id.short_hex(6)}@{self.endpoint_str})"
