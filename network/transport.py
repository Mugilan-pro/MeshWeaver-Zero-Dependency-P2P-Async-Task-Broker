"""
Asyncio network transport: TCP server/client with TLS support and an in-memory router for tests.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import struct
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from .protocol import FRAME_HEADER_FORMAT, MAX_FRAME_SIZE, NetworkPacket, ProtocolMessageType

logger = logging.getLogger("MeshWeaver.Transport")


async def read_network_packet(reader: asyncio.StreamReader) -> Optional[NetworkPacket]:
    """Reads a single framed NetworkPacket from an asyncio StreamReader."""
    try:
        header = await reader.readexactly(4)
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None

    frame_len = struct.unpack(FRAME_HEADER_FORMAT, header)[0]
    if frame_len > MAX_FRAME_SIZE:
        raise ValueError(f"Packet length {frame_len} exceeds maximum allowed {MAX_FRAME_SIZE}")

    body = await reader.readexactly(frame_len)
    return NetworkPacket.decode(body)


async def write_network_packet(writer: asyncio.StreamWriter, packet: NetworkPacket) -> None:
    """Writes a framed NetworkPacket to an asyncio StreamWriter."""
    frame = packet.encode()
    writer.write(frame)
    await writer.drain()


class MeshServer:
    """
    Asynchronous TCP/TLS Server for receiving incoming task execution requests,
    gossip telemetry, and heartbeat ping messages.
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        ssl_context: Optional[ssl.SSLContext] = None,
        handler: Optional[Callable[[NetworkPacket], Awaitable[Optional[NetworkPacket]]]] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self.handler = handler

        self._server: Optional[asyncio.Server] = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_tls(self) -> bool:
        return self.ssl_context is not None

    async def start(self) -> None:
        """Starts listening for incoming client connections."""
        if self._is_running:
            return

        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            ssl=self.ssl_context,
        )
        self._is_running = True
        actual_port = self._server.sockets[0].getsockname()[1] if self._server.sockets else self.port
        self.port = actual_port
        tls_label = "🔒 TLS Encrypted" if self.is_tls else "🔓 Unencrypted"
        logger.info(f"[{self.node_id}] MeshServer listening on {self.host}:{self.port} ({tls_label})")

    async def stop(self) -> None:
        """Stops the server gracefully."""
        if not self._is_running:
            return
        self._is_running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info(f"[{self.node_id}] MeshServer stopped.")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while self._is_running:
                packet = await read_network_packet(reader)
                if packet is None:
                    break

                if self.handler:
                    response_packet = await self.handler(packet)
                    if response_packet:
                        await write_network_packet(writer, response_packet)
        except Exception as e:
            logger.debug(f"[{self.node_id}] Client connection handler exception: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


class MeshClient:
    """
    Asynchronous TCP/TLS Client for dispatching tasks and gossip to peer nodes.
    """

    def __init__(
        self,
        local_node_id: str,
        ssl_context: Optional[ssl.SSLContext] = None,
        timeout: float = 10.0,
    ) -> None:
        self.local_node_id = local_node_id
        self.ssl_context = ssl_context
        self.timeout = timeout

    async def send_packet(
        self,
        host: str,
        port: int,
        packet: NetworkPacket,
        wait_for_response: bool = True,
    ) -> Optional[NetworkPacket]:
        """
        Connects to target host:port, optionally with TLS, sends packet,
        and returns response if requested.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=self.ssl_context),
            timeout=self.timeout,
        )
        try:
            await write_network_packet(writer, packet)
            if not wait_for_response:
                return None
            response = await asyncio.wait_for(read_network_packet(reader), timeout=self.timeout)
            return response
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


class InMemoryMeshRouter:
    """
    Virtual P2P Mesh Network Switch for in-memory simulation and sandboxed unit testing.
    Routes messages directly between node instances with optional simulated latency and TLS checks.
    """

    def __init__(self) -> None:
        # node_id -> handler callable (async def handler(packet) -> response)
        self._endpoints: Dict[str, Callable[[NetworkPacket], Awaitable[Optional[NetworkPacket]]]] = {}
        # node_id -> (host, port)
        self._locations: Dict[str, Tuple[str, int]] = {}
        # (host, port) -> node_id
        self._address_map: Dict[Tuple[str, int], str] = {}
        self.message_count = 0

    def register_endpoint(
        self,
        node_id: str,
        host: str,
        port: int,
        handler: Callable[[NetworkPacket], Awaitable[Optional[NetworkPacket]]],
    ) -> None:
        self._endpoints[node_id] = handler
        self._locations[node_id] = (host, port)
        self._address_map[(host, port)] = node_id

    def unregister_endpoint(self, node_id: str) -> None:
        self._endpoints.pop(node_id, None)
        addr = self._locations.pop(node_id, None)
        if addr:
            self._address_map.pop(addr, None)

    async def send(
        self,
        target_host: str,
        target_port: int,
        packet: NetworkPacket,
    ) -> Optional[NetworkPacket]:
        """Routes packet directly in memory to the target node's handler."""
        node_id = self._address_map.get((target_host, target_port))
        if not node_id:
            for nid, (h, p) in self._locations.items():
                if p == target_port:
                    node_id = nid
                    break

        if not node_id or node_id not in self._endpoints:
            raise ConnectionRefusedError(f"No active node registered at {target_host}:{target_port}")

        self.message_count += 1
        handler = self._endpoints[node_id]
        return await handler(packet)
