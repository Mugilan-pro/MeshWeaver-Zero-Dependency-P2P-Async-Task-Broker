"""
Packet wire protocol and framing for TCP/TLS communication.
"""

from __future__ import annotations

import enum
import json
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

FRAME_HEADER_FORMAT = "!I"  # 4-byte unsigned integer (big-endian) for payload length
MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB maximum single message


class ProtocolMessageType(str, enum.Enum):
    TASK_SUBMISSION = "TASK_SUBMISSION"
    TASK_RESULT = "TASK_RESULT"
    HEARTBEAT = "HEARTBEAT"
    METRICS_UPDATE = "METRICS_UPDATE"
    PING = "PING"
    PONG = "PONG"
    ERROR = "ERROR"


@dataclass
class NetworkPacket:
    """Wire envelope for network transmission."""
    message_type: ProtocolMessageType
    sender_id: str
    payload: Dict[str, Any] = field(default_factory=dict)
    binary_data: Optional[bytes] = None
    timestamp: float = field(default_factory=time.time)
    signature: Optional[str] = None

    def encode(self) -> bytes:
        """
        Encodes packet into framed byte buffer:
        [4-byte total length][JSON metadata length: 4-byte][JSON metadata bytes][Binary data]
        """
        meta = {
            "type": self.message_type.value,
            "sender_id": self.sender_id,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "signature": self.signature,
            "has_binary": self.binary_data is not None,
        }
        meta_bytes = json.dumps(meta).encode("utf-8")
        bin_bytes = self.binary_data or b""

        total_body = struct.pack("!I", len(meta_bytes)) + meta_bytes + bin_bytes
        frame = struct.pack(FRAME_HEADER_FORMAT, len(total_body)) + total_body
        return frame

    @classmethod
    def decode(cls, frame_body: bytes) -> NetworkPacket:
        """Decodes raw framed body (excluding the 4-byte length prefix)."""
        if len(frame_body) < 4:
            raise ValueError("Frame body too short to contain metadata length")

        meta_len = struct.unpack("!I", frame_body[:4])[0]
        meta_end = 4 + meta_len
        if len(frame_body) < meta_end:
            raise ValueError("Frame body truncated: missing metadata bytes")

        meta_bytes = frame_body[4:meta_end]
        binary_data = frame_body[meta_end:] if len(frame_body) > meta_end else None

        meta = json.loads(meta_bytes.decode("utf-8"))
        return cls(
            message_type=ProtocolMessageType(meta["type"]),
            sender_id=meta["sender_id"],
            payload=meta.get("payload", {}),
            binary_data=binary_data if meta.get("has_binary") else None,
            timestamp=float(meta.get("timestamp", time.time())),
            signature=meta.get("signature"),
        )
