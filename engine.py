"""
MeshWeaver Gossip Protocol Engine and Heartbeat Subsystem
Broadcasting CPU/RAM load metrics across peer nodes every 5 seconds.
Continuously monitors heartbeat expirations and instantly triggers task recovery
when a worker drops offline.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .telemetry import NodeTelemetrySnapshot, SystemMetricsCollector
from scheduler.router import TaskRouter, WorkerNodeInfo
from scheduler.reliability import ReliabilityManager

logger = logging.getLogger("MeshWeaver.Gossip.Engine")


class PeerHealthStatus(str, enum.Enum):
    ONLINE = "ONLINE"
    LAGGING = "LAGGING"
    DEAD = "DEAD"


@dataclass
class TrackedPeer:
    """Represents a peer's gossip telemetry and heartbeat state."""
    node_id: str
    host: str
    port: int
    cpu_percent: float = 0.0
    ram_percent: float = 0.0
    active_tasks: int = 0
    total_completed: int = 0
    last_heartbeat: float = field(default_factory=time.time)
    is_alive: bool = True

    def heartbeat_age(self) -> float:
        return max(0.0, time.time() - self.last_heartbeat)

    def health_status(self, timeout_seconds: float) -> PeerHealthStatus:
        if not self.is_alive:
            return PeerHealthStatus.DEAD
        age = self.heartbeat_age()
        if age > timeout_seconds:
            return PeerHealthStatus.DEAD
        elif age > (timeout_seconds * 0.6):
            return PeerHealthStatus.LAGGING
        return PeerHealthStatus.ONLINE


class GossipEngine:
    """
    Decentralized Gossip & Heartbeat Engine.
    Coordinates between DHT discovery, TaskRouter load balancing, and ReliabilityManager fault tolerance.
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        router: Optional[TaskRouter] = None,
        reliability: Optional[ReliabilityManager] = None,
        broadcast_interval_seconds: float = 5.0,
        heartbeat_timeout_seconds: float = 15.0,
        fanout: int = 3,
        metrics_collector: Optional[SystemMetricsCollector] = None,
        send_gossip_fn: Optional[Callable[[Dict[str, Any], Tuple[str, int]], Any]] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.router = router or TaskRouter(local_node_id=node_id)
        self.reliability = reliability or ReliabilityManager()
        self.broadcast_interval = broadcast_interval_seconds
        self.heartbeat_timeout = heartbeat_timeout_seconds
        self.fanout = fanout
        self.metrics = metrics_collector or SystemMetricsCollector(node_id, host, port)
        self.send_gossip_fn = send_gossip_fn

        # Peer registry: node_id -> TrackedPeer
        self._tracked_peers: Dict[str, TrackedPeer] = {}

        # Background loops
        self._broadcast_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._is_running = False

        # Event hooks for dashboard and logging
        self.on_peer_updated: Optional[Callable[[TrackedPeer], None]] = None
        self.on_peer_dropout: Optional[Callable[[str], None]] = None

    @property
    def is_running(self) -> bool:
        return self._is_running

    def register_peer(self, node_id: str, host: str, port: int) -> TrackedPeer:
        """Explicitly registers or updates an endpoint in the gossip registry."""
        if node_id == self.node_id:
            return None  # type: ignore

        if node_id not in self._tracked_peers:
            peer = TrackedPeer(
                node_id=node_id,
                host=host,
                port=port,
                last_heartbeat=time.time(),
                is_alive=True,
            )
            self._tracked_peers[node_id] = peer
            self.router.update_worker_metrics(
                node_id=node_id,
                cpu_percent=0.0,
                ram_percent=0.0,
                active_tasks=0,
                host=host,
                port=port,
            )
        return self._tracked_peers[node_id]

    def handle_incoming_gossip(self, data: Dict[str, Any]) -> None:
        """
        Processes an incoming gossip message from a peer.
        Refreshes heartbeat timestamp and updates the router's load profile.
        """
        sender_id = data.get("node_id")
        if not sender_id or sender_id == self.node_id:
            return

        host = data.get("host", "127.0.0.1")
        port = int(data.get("port", 8000))
        cpu_percent = float(data.get("cpu_percent", 0.0))
        ram_percent = float(data.get("ram_percent", 0.0))
        active_tasks = int(data.get("active_tasks", 0))
        total_completed = int(data.get("total_completed", 0))

        now = time.time()
        peer = self._tracked_peers.get(sender_id)
        if not peer:
            peer = TrackedPeer(
                node_id=sender_id,
                host=host,
                port=port,
                cpu_percent=cpu_percent,
                ram_percent=ram_percent,
                active_tasks=active_tasks,
                total_completed=total_completed,
                last_heartbeat=now,
                is_alive=True,
            )
            self._tracked_peers[sender_id] = peer
        else:
            peer.host = host
            peer.port = port
            peer.cpu_percent = cpu_percent
            peer.ram_percent = ram_percent
            peer.active_tasks = active_tasks
            peer.total_completed = total_completed
            peer.last_heartbeat = now
            peer.is_alive = True

        # Keep TaskRouter updated with fresh CPU metrics
        self.router.update_worker_metrics(
            node_id=sender_id,
            cpu_percent=cpu_percent,
            ram_percent=ram_percent,
            active_tasks=active_tasks,
            host=host,
            port=port,
        )

        if self.on_peer_updated:
            self.on_peer_updated(peer)

    def check_heartbeats(self) -> List[str]:
        """
        Evaluates heartbeat freshness of all peers.
        Identifies peers that exceeded timeout, flags them DEAD, and triggers fault recovery.
        Returns a list of newly dropped node IDs.
        """
        now = time.time()
        dropped_node_ids: List[str] = []

        for node_id, peer in list(self._tracked_peers.items()):
            if peer.is_alive and (now - peer.last_heartbeat) > self.heartbeat_timeout:
                peer.is_alive = False
                dropped_node_ids.append(node_id)
                logger.warning(
                    f"[{self.node_id}] Heartbeat lost for peer {node_id} "
                    f"(silence: {now - peer.last_heartbeat:.1f}s > {self.heartbeat_timeout:.1f}s). Flagging DEAD."
                )

                # 1. Update task router so no new tasks route to dead node
                self.router.mark_worker_dead(node_id)

                # 2. Trigger automatic instant task reassignment in ReliabilityManager
                reassigned = self.reliability.handle_worker_dropout(node_id, self.router)
                logger.info(
                    f"[{self.node_id}] Fault tolerance re-routed {len(reassigned)} tasks orphaned by {node_id}"
                )

                if self.on_peer_dropout:
                    self.on_peer_dropout(node_id)

        return dropped_node_ids

    def get_cluster_snapshot(self) -> List[Dict[str, Any]]:
        """Returns current health and resource status of all peers."""
        result = []
        for peer in self._tracked_peers.values():
            result.append({
                "node_id": peer.node_id,
                "host": peer.host,
                "port": peer.port,
                "cpu_percent": peer.cpu_percent,
                "ram_percent": peer.ram_percent,
                "active_tasks": peer.active_tasks,
                "total_completed": peer.total_completed,
                "last_heartbeat": peer.last_heartbeat,
                "heartbeat_age": peer.heartbeat_age(),
                "status": peer.health_status(self.heartbeat_timeout).value,
                "is_alive": peer.is_alive,
            })
        return result

    async def start(self) -> None:
        """Starts asynchronous gossip broadcast and heartbeat monitoring loops."""
        if self._is_running:
            return
        self._is_running = True
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())
        self._monitor_task = asyncio.create_task(self._heartbeat_monitor_loop())
        logger.info(f"[{self.node_id}] Gossip & Heartbeat Engine started.")

    async def stop(self) -> None:
        """Gracefully cancels background tasks."""
        self._is_running = False
        if self._broadcast_task:
            self._broadcast_task.cancel()
        if self._monitor_task:
            self._monitor_task.cancel()
        logger.info(f"[{self.node_id}] Gossip Engine stopped.")

    async def _broadcast_loop(self) -> None:
        """Broadcasts local load telemetry every 5 seconds to nearest neighbors."""
        while self._is_running:
            try:
                snapshot = self.metrics.capture_snapshot()
                data = snapshot.to_dict()

                # Pick random subset of peers up to fanout
                alive_peers = [p for p in self._tracked_peers.values() if p.is_alive]
                if alive_peers and self.send_gossip_fn:
                    targets = random.sample(alive_peers, min(self.fanout, len(alive_peers)))
                    for target in targets:
                        try:
                            res = self.send_gossip_fn(data, (target.host, target.port))
                            if asyncio.iscoroutine(res):
                                await res
                        except Exception as e:
                            logger.debug(f"Failed to send gossip to {target.node_id}: {e}")

                await asyncio.sleep(self.broadcast_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip broadcast loop: {e}")
                await asyncio.sleep(1.0)

    async def _heartbeat_monitor_loop(self) -> None:
        """Periodically sweeps peer heartbeats to detect dropouts."""
        sweep_interval = min(2.0, self.heartbeat_timeout / 3.0)
        while self._is_running:
            try:
                self.check_heartbeats()
                await asyncio.sleep(sweep_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat monitor loop: {e}")
                await asyncio.sleep(1.0)
