"""
network/protocol.py
Part of MeshWeaver: Zero-Dependency P2P Async Task Broker
Role: Member 2 (P2P Networking Developer)

Implements length-prefixed message framing over TCP streams using standard library only.
Prefixes all JSON payloads with a 4-byte big-endian unsigned integer indicating payload length.
This completely prevents TCP fragmentation and stream coalescing issues.
"""

import asyncio
import json
import logging
import struct
from typing import Any, Dict, Optional

logger = logging.getLogger("MeshWeaver.Network.Protocol")


class MessageProtocol:
    """
    Handles framing, encoding, and decoding of peer-to-peer network messages.

    Framing Structure:
    +-----------------------------+------------------------------------+
    | Header: 4 bytes ('!I')      | Payload: N bytes (UTF-8 JSON)      |
    | (Unsigned 32-bit big-endian)|                                    |
    +-----------------------------+------------------------------------+
    """

    # 4-byte unsigned integer in network byte order (big-endian)
    HEADER_FORMAT: str = "!I"
    HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)  # Exactly 4 bytes

    # Safety limit: 16 MB maximum message payload to prevent memory exhaustion / DoS
    MAX_PAYLOAD_SIZE: int = 16 * 1024 * 1024

    @staticmethod
    async def encode_message(data_dict: Dict[str, Any]) -> bytes:
        """
        Asynchronously serializes a Python dictionary to JSON UTF-8 bytes and
        prefixes it with a 4-byte big-endian length header.

        Args:
            data_dict: Dictionary containing the payload to transmit.

        Returns:
            bytes: The complete wire-ready frame (4-byte header + UTF-8 JSON payload).

        Raises:
            TypeError: If data_dict is not JSON serializable.
            ValueError: If payload length exceeds MAX_PAYLOAD_SIZE.
        """
        return MessageProtocol.encode_message_sync(data_dict)

    @staticmethod
    def encode_message_sync(data_dict: Dict[str, Any]) -> bytes:
        """
        Synchronous utility to encode a message dictionary into framed bytes.
        Useful when preparing messages outside of coroutine contexts.

        Args:
            data_dict: Dictionary containing the payload.

        Returns:
            bytes: Framed bytes ready for writer.write().
        """
        # Serialize to JSON with UTF-8 encoding
        payload_bytes = json.dumps(data_dict, ensure_ascii=False).encode("utf-8")
        payload_length = len(payload_bytes)

        if payload_length > MessageProtocol.MAX_PAYLOAD_SIZE:
            raise ValueError(
                f"Payload size ({payload_length} bytes) exceeds maximum "
                f"allowed limit ({MessageProtocol.MAX_PAYLOAD_SIZE} bytes)."
            )

        # Pack length into 4 bytes big-endian
        header_bytes = struct.pack(MessageProtocol.HEADER_FORMAT, payload_length)
        return header_bytes + payload_bytes

    @staticmethod
    async def read_message(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
        """
        Reads a single framed message from an asyncio.StreamReader.

        Guarantees that full messages are assembled by reading the exact length
        specified in the 4-byte header, resolving TCP stream fragmentation.

        Args:
            reader: Active asyncio.StreamReader connected to a peer.

        Returns:
            Optional[Dict[str, Any]]: Parsed JSON dictionary if read successfully,
                                      or None if the connection dropped, EOF reached,
                                      or the message was malformed.
        """
        # Step 1: Read the 4-byte length prefix header
        try:
            header_bytes = await reader.readexactly(MessageProtocol.HEADER_SIZE)
        except asyncio.IncompleteReadError as exc:
            if not exc.partial:
                # Normal EOF: connection closed cleanly before new data
                logger.debug("Peer closed connection cleanly (EOF at header).")
                return None
            logger.warning(
                "Peer disconnected mid-header: received %d of %d expected bytes.",
                len(exc.partial),
                MessageProtocol.HEADER_SIZE,
            )
            return None
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, EOFError) as exc:
            logger.debug("Socket dropped during header read: %s", exc)
            return None

        # Step 2: Unpack the payload length
        (payload_length,) = struct.unpack(MessageProtocol.HEADER_FORMAT, header_bytes)

        # Validate payload length bounds
        if payload_length > MessageProtocol.MAX_PAYLOAD_SIZE:
            logger.error(
                "Payload size %d exceeds safety limit (%d bytes). Rejecting frame.",
                payload_length,
                MessageProtocol.MAX_PAYLOAD_SIZE,
            )
            return None

        if payload_length == 0:
            logger.warning("Received 0-byte payload frame.")
            return {}

        # Step 3: Read exactly `payload_length` bytes
        try:
            payload_bytes = await reader.readexactly(payload_length)
        except asyncio.IncompleteReadError as exc:
            logger.warning(
                "Peer disconnected mid-payload: expected %d bytes, got %d before EOF.",
                payload_length,
                len(exc.partial),
            )
            return None
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, EOFError) as exc:
            logger.debug("Socket dropped during payload read: %s", exc)
            return None

        # Step 4: Decode UTF-8 and parse JSON
        try:
            payload_str = payload_bytes.decode("utf-8")
            data = json.loads(payload_str)
            if not isinstance(data, dict):
                logger.warning(
                    "Expected JSON object (dict), received %s.", type(data).__name__
                )
            return data
        except UnicodeDecodeError as exc:
            logger.error("Failed to decode payload as UTF-8: %s", exc)
            return None
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse payload as JSON: %s", exc)
            return None
