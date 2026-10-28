"""
MeshWeaver Wire Protocol Framing and Serialization
Implements length-prefixed framing over TCP/TLS streams for asynchronous packet exchange.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any, Dict, Optional

MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB max payload limit
HEADER_STRUCT = struct.Struct("!I")  # 4-byte big-endian unsigned integer


class ProtocolError(Exception):
    """Raised when wire packet framing is corrupted or exceeds buffer bounds."""
    pass


class MessageType:
    TASK_EXECUTE_REQUEST = "TASK_EXECUTE_REQUEST"
    TASK_EXECUTE_RESPONSE = "TASK_EXECUTE_RESPONSE"
    GOSSIP_SNAPSHOT = "GOSSIP_SNAPSHOT"
    GOSSIP_ACK = "GOSSIP_ACK"
    PING = "PING"
    PONG = "PONG"


async def read_frame(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
    """
    Reads a single length-prefixed JSON frame from an asyncio StreamReader.
    Returns None if the connection is closed cleanly before reading data.
    """
    try:
        header = await reader.readexactly(HEADER_STRUCT.size)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None

    length = HEADER_STRUCT.unpack(header)[0]
    if length > MAX_FRAME_SIZE:
        raise ProtocolError(f"Frame length {length} bytes exceeds maximum allowed {MAX_FRAME_SIZE} bytes")

    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as e:
        raise ProtocolError(f"Stream truncated while reading frame body ({len(e.partial)}/{length} bytes)") from e

    try:
        return json.loads(body.decode("utf-8"))
    except Exception as e:
        raise ProtocolError(f"Failed to parse JSON packet body: {e}") from e


async def write_frame(writer: asyncio.StreamWriter, data: Dict[str, Any]) -> None:
    """
    Encodes data as JSON, prefixes with a 4-byte length header, and writes to StreamWriter.
    """
    encoded = json.dumps(data).encode("utf-8")
    header = HEADER_STRUCT.pack(len(encoded))
    writer.write(header + encoded)
    await writer.drain()
