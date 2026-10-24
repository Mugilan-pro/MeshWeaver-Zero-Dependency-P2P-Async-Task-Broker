"""
Asyncio UDP peer discovery and bootstrap protocol for MeshWeaver DHT.
Implements Kademlia RPCs (PING, PONG, FIND_NODE, FOUND_NODES) and iterative lookups.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .node_id import NodeID, NodeContact, DEFAULT_ID_BITS, xor_distance
from .routing import RoutingTable

logger = logging.getLogger("MeshWeaver.DHTDiscovery")


class RPCMessageType:
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    FOUND_NODES = "FOUND_NODES"


class DHTProtocol(asyncio.DatagramProtocol):
    """
    Handles UDP datagrams for Kademlia RPCs using asyncio.
    Passes decoded messages to the parent DHTDiscoveryNode.
    """

    def __init__(self, node: DHTDiscoveryNode) -> None:
        self.node = node
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:  # type: ignore[override]
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            message = json.loads(data.decode("utf-8"))
            self.node.handle_incoming_datagram(message, addr)
        except Exception as e:
            logger.debug(f"Could not parse message from {addr}: {e}")

    def error_received(self, exc: Exception) -> None:
        logger.debug(f"UDP socket error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        pass


class DHTDiscoveryNode:
    """
    A Kademlia DHT node listening on a UDP port.
    Can ping peers, respond to node queries, bootstrap into an existing mesh,
    and iteratively explore the network.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        node_id: Optional[NodeID] = None,
        k: int = 20,
        alpha: int = 3,
        rpc_timeout: float = 3.0,
    ) -> None:
        self.host = host
        self.port = port
        self.node_id = node_id or NodeID.generate()
        self.k = k
        self.alpha = alpha
        self.rpc_timeout = rpc_timeout

        # Local routing table
        self.routing_table = RoutingTable(local_node_id=self.node_id, k=self.k)

        # Transport & inflight RPC tracker
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.protocol: Optional[DHTProtocol] = None
        self._pending_rpcs: Dict[str, Tuple[asyncio.Future, float, Tuple[str, int]]] = {}
        self._is_running = False

        # Counters for stats
        self.pings_sent = 0
        self.pings_received = 0
        self.find_node_sent = 0
        self.find_node_received = 0
        self.total_nodes_discovered = 0

    @property
    def is_running(self) -> bool:
        return self._is_running and self.transport is not None

    @property
    def endpoint(self) -> Tuple[str, int]:
        return self.host, self.port

    @property
    def endpoint_str(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def self_contact(self) -> NodeContact:
        return NodeContact(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
            last_seen=time.time(),
        )

    async def start(self) -> None:
        """Start listening on the UDP port."""
        if self._is_running:
            return

        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: DHTProtocol(self),
            local_addr=(self.host, self.port),
        )
        self.transport = transport
        self.protocol = protocol

        # If port 0 was passed, OS gave us an ephemeral port; grab it
        sock = transport.get_extra_info("socket")
        if sock:
            actual_addr = sock.getsockname()
            self.host = actual_addr[0]
            self.port = actual_addr[1]

        self._is_running = True
        logger.info(f"DHT node [{self.node_id.short_hex(6)}] listening on UDP {self.host}:{self.port}")

    async def stop(self) -> None:
        """Close UDP socket and cancel any waiting RPC requests."""
        self._is_running = False

        for rpc_id, (fut, _, _) in list(self._pending_rpcs.items()):
            if not fut.done():
                fut.cancel()
        self._pending_rpcs.clear()

        if self.transport:
            self.transport.close()
            self.transport = None
            self.protocol = None

        logger.info(f"DHT node [{self.node_id.short_hex(6)}] stopped.")

    def _send_datagram(self, payload: Dict[str, Any], addr: Tuple[str, int]) -> None:
        if not self.transport or self.transport.is_closing():
            return
        data = json.dumps(payload).encode("utf-8")
        self.transport.sendto(data, addr)

    def handle_incoming_datagram(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Dispatch incoming RPC and record sender in our routing table."""
        msg_type = msg.get("type")
        rpc_id = msg.get("rpc_id")
        sender_hex = msg.get("sender_id")

        if not msg_type or not rpc_id or not sender_hex:
            return

        try:
            sender_id = NodeID.from_hex(sender_hex)
        except Exception:
            return

        sender_host = msg.get("sender_host", addr[0])
        sender_port = int(msg.get("sender_port", addr[1]))

        # Every incoming message confirms the sender is alive: update our routing table
        if sender_id != self.node_id:
            sender_contact = NodeContact(node_id=sender_id, host=sender_host, port=sender_port)
            inserted, _ = self.routing_table.add_or_update(sender_contact)
            if inserted:
                self.total_nodes_discovered += 1

        # Handle specific message types
        if msg_type == RPCMessageType.PING:
            self.pings_received += 1
            reply = {
                "type": RPCMessageType.PONG,
                "rpc_id": rpc_id,
                "sender_id": self.node_id.hex_value,
                "sender_host": self.host,
                "sender_port": self.port,
            }
            self._send_datagram(reply, addr)

        elif msg_type == RPCMessageType.PONG:
            if rpc_id in self._pending_rpcs:
                fut, start_time, _ = self._pending_rpcs.pop(rpc_id)
                if not fut.done():
                    rtt_ms = (time.perf_counter() - start_time) * 1000.0
                    self.routing_table.touch(sender_id, rtt_ms=rtt_ms)
                    fut.set_result((True, rtt_ms, sender_id))

        elif msg_type == RPCMessageType.FIND_NODE:
            self.find_node_received += 1
            target_hex = msg.get("target_id")
            if target_hex:
                try:
                    target_id = NodeID.from_hex(target_hex)
                except Exception:
                    target_id = sender_id
            else:
                target_id = sender_id

            # Grab the k closest nodes we know of
            closest = self.routing_table.find_closest_nodes(
                target_id=target_id,
                count=self.k,
                exclude_ids={sender_id},
            )
            reply = {
                "type": RPCMessageType.FOUND_NODES,
                "rpc_id": rpc_id,
                "sender_id": self.node_id.hex_value,
                "sender_host": self.host,
                "sender_port": self.port,
                "nodes": [c.to_dict() for c in closest],
            }
            self._send_datagram(reply, addr)

        elif msg_type == RPCMessageType.FOUND_NODES:
            if rpc_id in self._pending_rpcs:
                fut, _, _ = self._pending_rpcs.pop(rpc_id)
                raw_nodes = msg.get("nodes", [])
                discovered: List[NodeContact] = []
                for item in raw_nodes:
                    try:
                        c = NodeContact.from_dict(item)
                        if c.node_id != self.node_id:
                            self.routing_table.add_or_update(c)
                            discovered.append(c)
                    except Exception:
                        continue
                if not fut.done():
                    fut.set_result(discovered)

    async def ping(
        self,
        target: Union[NodeContact, Tuple[str, int]],
        timeout: Optional[float] = None,
    ) -> Tuple[bool, float, Optional[NodeID]]:
        """
        Send a PING to target. Returns (success, rtt_ms, peer_node_id).
        """
        if not self.is_running:
            raise RuntimeError("Start node before sending pings.")

        if isinstance(target, NodeContact):
            addr = target.endpoint
            expected_id = target.node_id
        else:
            addr = target
            expected_id = None

        rpc_id = uuid.uuid4().hex
        payload = {
            "type": RPCMessageType.PING,
            "rpc_id": rpc_id,
            "sender_id": self.node_id.hex_value,
            "sender_host": self.host,
            "sender_port": self.port,
        }

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        start_time = time.perf_counter()
        self._pending_rpcs[rpc_id] = (fut, start_time, addr)
        self.pings_sent += 1

        self._send_datagram(payload, addr)

        wait_sec = timeout or self.rpc_timeout
        try:
            return await asyncio.wait_for(fut, timeout=wait_sec)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            self._pending_rpcs.pop(rpc_id, None)
            if expected_id:
                # If a known node failed to respond, remove it
                self.routing_table.remove(expected_id)
            return False, 0.0, None

    async def find_node(
        self,
        contact: NodeContact,
        target_id: NodeID,
        timeout: Optional[float] = None,
    ) -> List[NodeContact]:
        """Ask a peer for the closest nodes it knows to target_id."""
        if not self.is_running:
            raise RuntimeError("Start node before querying.")

        rpc_id = uuid.uuid4().hex
        payload = {
            "type": RPCMessageType.FIND_NODE,
            "rpc_id": rpc_id,
            "sender_id": self.node_id.hex_value,
            "sender_host": self.host,
            "sender_port": self.port,
            "target_id": target_id.hex_value,
        }

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        start_time = time.perf_counter()
        self._pending_rpcs[rpc_id] = (fut, start_time, contact.endpoint)
        self.find_node_sent += 1

        self._send_datagram(payload, contact.endpoint)

        wait_sec = timeout or self.rpc_timeout
        try:
            return await asyncio.wait_for(fut, timeout=wait_sec)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            self._pending_rpcs.pop(rpc_id, None)
            return []

    async def bootstrap(
        self,
        bootstrap_endpoints: List[Union[Tuple[str, int], NodeContact]],
        timeout_per_peer: Optional[float] = None,
    ) -> bool:
        """
        Join an existing network:
        1. Ping known bootstrap address(es) to learn their NodeIDs.
        2. Insert responsive bootstrap nodes into our routing table.
        3. Search for nodes closest to our own ID to discover neighbors.
        """
        if not self.is_running:
            await self.start()

        successful = 0
        for entry in bootstrap_endpoints:
            success, rtt, peer_id = await self.ping(entry, timeout=timeout_per_peer)
            if success and peer_id:
                host, port = entry.endpoint if isinstance(entry, NodeContact) else entry
                contact = NodeContact(node_id=peer_id, host=host, port=port, rtt_ms=rtt)
                self.routing_table.add_or_update(contact)
                successful += 1

        if successful == 0:
            return False

        # Look up our own ID to populate nearest buckets
        await self.iterative_find_node(self.node_id)
        return True

    async def iterative_find_node(
        self,
        target_id: NodeID,
        count: Optional[int] = None,
    ) -> List[NodeContact]:
        """
        Kademlia alpha-concurrent iterative lookup for target_id:
        Query alpha closest unqueried peers, merge responses into shortlist, repeat.
        """
        target_count = count or self.k

        shortlist = self.routing_table.find_closest_nodes(target_id=target_id, count=self.k)
        if not shortlist:
            return []

        visited: Set[NodeID] = set()
        closest_dist = shortlist[0].node_id.xor_distance(target_id)

        while True:
            # Pick alpha unqueried nodes closest to target
            unvisited = [c for c in shortlist if c.node_id not in visited]
            if not unvisited:
                break

            batch = unvisited[:self.alpha]
            for c in batch:
                visited.add(c.node_id)

            # Query concurrently
            tasks = [self.find_node(c, target_id) for c in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            found_new = False
            for res in results:
                if isinstance(res, list):
                    for new_c in res:
                        if new_c.node_id != self.node_id:
                            self.routing_table.add_or_update(new_c)
                            if not any(c.node_id == new_c.node_id for c in shortlist):
                                shortlist.append(new_c)
                                found_new = True

            shortlist.sort(key=lambda c: c.node_id.xor_distance(target_id))
            shortlist = shortlist[:self.k]

            current_dist = shortlist[0].node_id.xor_distance(target_id)
            if not found_new and current_dist >= closest_dist:
                break

            closest_dist = min(closest_dist, current_dist)

        return shortlist[:target_count]

    def get_known_peers(self) -> List[NodeContact]:
        return self.routing_table.get_all_contacts()

    def export_worker_candidates(self) -> List[Dict[str, Any]]:
        """Export peer list for Member 4 TaskRouter."""
        return [
            {
                "node_id": c.node_id.hex_value,
                "host": c.host,
                "port": c.port,
                "rtt_ms": c.rtt_ms,
            }
            for c in self.get_known_peers()
        ]

    def telemetry_snapshot(self) -> Dict[str, Any]:
        """Summary for Member 5 CLI dashboard."""
        return {
            "node_id": self.node_id.hex_value,
            "short_id": self.node_id.short_hex(8),
            "endpoint": f"{self.host}:{self.port}",
            "is_running": self.is_running,
            "total_contacts": self.routing_table.total_contacts,
            "pings_sent": self.pings_sent,
            "pings_received": self.pings_received,
            "find_node_sent": self.find_node_sent,
            "find_node_received": self.find_node_received,
            "total_nodes_discovered": self.total_nodes_discovered,
        }

    def __repr__(self) -> str:
        return f"DHTDiscoveryNode({self.node_id.short_hex(6)}@{self.endpoint_str}, peers={self.routing_table.total_contacts})"
