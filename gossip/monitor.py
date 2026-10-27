"""
Gossip Protocol and Failure Detection Monitor for MeshWeaver.

Runs background tasks that:
1. Sample local CPU/RAM metrics.
2. Broadcast metrics to random neighbors every 5 seconds (configurable).
3. Track heartbeats and mark unresponsive peers as SUSPECT or DEAD.
4. Notify the scheduler to re-assign tasks if a worker drops out.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from config import (
    GOSSIP_FANOUT,
    HEARTBEAT_INTERVAL_SECONDS,
    HEARTBEAT_TIMEOUT_SECONDS,
)
from .metrics import SystemMetricsCollector
from .models import (
    GossipDigest,
    GossipMessage,
    GossipMessageType,
    NodeMetrics,
    PeerHealthState,
    PeerNodeRecord,
)

logger = logging.getLogger("MeshWeaver.GossipMonitor")


class GossipMonitor:
    """Manages gossip exchange, peer membership, and failure detection."""

    def __init__(
        self,
        local_node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        gossip_interval: float = HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_timeout: float = HEARTBEAT_TIMEOUT_SECONDS,
        fanout: int = GOSSIP_FANOUT,
        metrics_collector: Optional[SystemMetricsCollector] = None,
        transport_send_fn: Optional[Callable[[str, int, bytes], Awaitable[None]]] = None,
    ) -> None:
        self.local_node_id = local_node_id
        self.host = host
        self.port = port
        self.gossip_interval = gossip_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.suspect_timeout = heartbeat_timeout / 2.0  # Missed heartbeat threshold
        self.fanout = fanout

        # Pluggable networking callback: async def send(host, port, data_bytes)
        self.transport_send_fn = transport_send_fn

        self.metrics_collector = metrics_collector or SystemMetricsCollector(node_id=local_node_id)
        self.local_generation = int(time.time())
        self.local_sequence = 0

        # Known peers table: node_id -> PeerNodeRecord
        self._peers: Dict[str, PeerNodeRecord] = {}

        # Callbacks for node events
        self.on_peer_dropout: Optional[Callable[[str], None]] = None
        self.on_peer_recovered: Optional[Callable[[str], None]] = None
        self.on_peer_suspect: Optional[Callable[[str], None]] = None
        self.on_metrics_updated: Optional[Callable[[str, NodeMetrics], None]] = None

        self.stats = {
            "gossip_rounds": 0,
            "messages_sent": 0,
            "messages_received": 0,
            "heartbeats_sent": 0,
            "heartbeats_received": 0,
            "anti_entropy_syncs": 0,
            "dropouts_detected": 0,
            "recoveries_detected": 0,
        }

        self._is_running = False
        self._tasks: List[asyncio.Task] = []
        self._udp_transport: Optional[asyncio.DatagramTransport] = None

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def peer_count(self) -> int:
        return len(self._peers)

    @property
    def live_peer_count(self) -> int:
        return sum(1 for p in self._peers.values() if p.is_alive)

    @property
    def healthy_peer_count(self) -> int:
        return sum(1 for p in self._peers.values() if p.is_healthy)

    @property
    def dead_peer_count(self) -> int:
        return sum(1 for p in self._peers.values() if p.is_dead)

    # -------------------------------------------------------------------------
    # Peer Management
    # -------------------------------------------------------------------------

    def add_peer(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        initial_metrics: Optional[NodeMetrics] = None,
    ) -> PeerNodeRecord:
        """Adds or updates a peer in the local directory."""
        if node_id == self.local_node_id:
            logger.debug("Local node cannot be added as peer to itself")
            return None  # type: ignore

        if node_id in self._peers:
            record = self._peers[node_id]
            record.host = host
            record.port = port
            if initial_metrics:
                record.metrics = initial_metrics
            return record

        metrics = initial_metrics or NodeMetrics(node_id=node_id)
        record = PeerNodeRecord(
            node_id=node_id,
            host=host,
            port=port,
            metrics=metrics,
            health_state=PeerHealthState.HEALTHY,
            last_heartbeat_time=time.time(),
            heartbeat_sequence=0,
            generation=0,
        )
        self._peers[node_id] = record
        logger.info(f"[{self.local_node_id}] Added peer {node_id} ({host}:{port})")
        return record

    def remove_peer(self, node_id: str) -> Optional[PeerNodeRecord]:
        return self._peers.pop(node_id, None)

    def get_peer(self, node_id: str) -> Optional[PeerNodeRecord]:
        return self._peers.get(node_id)

    def get_live_peers(self) -> List[PeerNodeRecord]:
        return [p for p in self._peers.values() if p.is_alive]

    def get_dead_peers(self) -> List[PeerNodeRecord]:
        return [p for p in self._peers.values() if p.is_dead]

    def get_all_peers(self) -> List[PeerNodeRecord]:
        return list(self._peers.values())

    def get_health_summary(self) -> Dict[str, Any]:
        return {
            "local_node_id": self.local_node_id,
            "total_peers": len(self._peers),
            "healthy_peers": self.healthy_peer_count,
            "suspect_peers": sum(1 for p in self._peers.values() if p.is_suspect),
            "dead_peers": self.dead_peer_count,
            "stats": self.stats.copy(),
            "local_metrics": self.metrics_collector.collect().to_dict(),
        }

    # -------------------------------------------------------------------------
    # Anti-Entropy & State Synchronization
    # -------------------------------------------------------------------------

    def create_state_digest(self) -> GossipDigest:
        """Creates a digest of known peer version numbers."""
        local_metrics = self.metrics_collector.collect()
        entries: Dict[str, Tuple[int, int, float]] = {
            self.local_node_id: (self.local_generation, self.local_sequence, local_metrics.timestamp)
        }
        for node_id, peer in self._peers.items():
            entries[node_id] = (peer.generation, peer.heartbeat_sequence, peer.metrics.timestamp)

        return GossipDigest(
            sender_id=self.local_node_id,
            entries=entries,
            timestamp=time.time(),
        )

    def compute_anti_entropy_delta(self, remote_digest: GossipDigest) -> Dict[str, Dict[str, Any]]:
        """Finds records where this node has newer data than the remote digest."""
        delta: Dict[str, Dict[str, Any]] = {}
        local_metrics = self.metrics_collector.collect()

        # Check local node record
        remote_local_entry = remote_digest.entries.get(self.local_node_id)
        if not remote_local_entry or remote_local_entry[0] < self.local_generation or remote_local_entry[1] < self.local_sequence:
            delta[self.local_node_id] = {
                "host": self.host,
                "port": self.port,
                "metrics": local_metrics.to_dict(),
                "generation": self.local_generation,
                "sequence": self.local_sequence,
            }

        # Check known peer records
        for node_id, peer in self._peers.items():
            remote_entry = remote_digest.entries.get(node_id)
            if not remote_entry:
                delta[node_id] = {
                    "host": peer.host,
                    "port": peer.port,
                    "metrics": peer.metrics.to_dict(),
                    "generation": peer.generation,
                    "sequence": peer.heartbeat_sequence,
                }
            else:
                rem_gen, rem_seq, _ = remote_entry
                if peer.generation > rem_gen or (peer.generation == rem_gen and peer.heartbeat_sequence > rem_seq):
                    delta[node_id] = {
                        "host": peer.host,
                        "port": peer.port,
                        "metrics": peer.metrics.to_dict(),
                        "generation": peer.generation,
                        "sequence": peer.heartbeat_sequence,
                    }

        return delta

    def apply_anti_entropy_delta(self, delta: Dict[str, Dict[str, Any]]) -> int:
        """Updates local peer records with newer state received in a delta."""
        updated_count = 0
        for node_id, record_data in delta.items():
            if node_id == self.local_node_id:
                continue

            generation = record_data.get("generation", 0)
            sequence = record_data.get("sequence", 0)
            metrics_dict = record_data.get("metrics", {})
            host = record_data.get("host", "127.0.0.1")
            port = record_data.get("port", 8000)

            peer = self._peers.get(node_id)
            if peer is None:
                # Newly discovered node
                metrics = NodeMetrics.from_dict(metrics_dict)
                new_peer = self.add_peer(node_id, host=host, port=port, initial_metrics=metrics)
                if new_peer:
                    new_peer.generation = generation
                    new_peer.heartbeat_sequence = sequence
                    new_peer.last_heartbeat_time = time.time()
                updated_count += 1
            else:
                # Update existing peer if incoming sequence is newer
                is_newer_gen = generation > peer.generation
                is_newer_seq = (generation == peer.generation and sequence > peer.heartbeat_sequence)
                if is_newer_gen or is_newer_seq:
                    peer.generation = generation
                    peer.heartbeat_sequence = sequence
                    peer.metrics = NodeMetrics.from_dict(metrics_dict)
                    peer.last_heartbeat_time = time.time()
                    if peer.is_dead:
                        peer.health_state = PeerHealthState.HEALTHY
                        self.stats["recoveries_detected"] += 1
                        if self.on_peer_recovered:
                            self.on_peer_recovered(node_id)
                    updated_count += 1

                    if self.on_metrics_updated:
                        self.on_metrics_updated(node_id, peer.metrics)

        return updated_count

    # -------------------------------------------------------------------------
    # Message Dispatch & Handling
    # -------------------------------------------------------------------------

    async def handle_incoming_message(self, message: GossipMessage) -> Optional[GossipMessage]:
        """Handles an incoming gossip message envelope."""
        self.stats["messages_received"] += 1
        sender_id = message.sender_id

        if sender_id == self.local_node_id:
            return None

        # Auto-register sender if not in directory
        sender_peer = self._peers.get(sender_id)
        if sender_peer is None:
            host = message.payload.get("host", "127.0.0.1")
            port = message.payload.get("port", 8000)
            sender_peer = self.add_peer(sender_id, host=host, port=port)

        now = time.time()

        if message.message_type == GossipMessageType.HEARTBEAT:
            self.stats["heartbeats_received"] += 1
            was_dead = sender_peer.is_dead
            sender_peer.record_heartbeat(
                sequence=message.sequence_number,
                generation=message.payload.get("generation", 0),
            )
            if was_dead:
                self.stats["recoveries_detected"] += 1
                logger.info(f"[{self.local_node_id}] Dead peer {sender_id} RECOVERED via heartbeat")
                if self.on_peer_recovered:
                    self.on_peer_recovered(sender_id)

        elif message.message_type == GossipMessageType.METRICS_UPDATE:
            self.stats["heartbeats_received"] += 1
            metrics_data = message.payload.get("metrics")
            metrics = NodeMetrics.from_dict(metrics_data) if metrics_data else None
            was_dead = sender_peer.is_dead

            sender_peer.record_heartbeat(
                sequence=message.sequence_number,
                generation=message.payload.get("generation", 0),
                metrics=metrics,
            )

            if was_dead:
                self.stats["recoveries_detected"] += 1
                logger.info(f"[{self.local_node_id}] Peer {sender_id} RECOVERED via metrics update")
                if self.on_peer_recovered:
                    self.on_peer_recovered(sender_id)

            if metrics and self.on_metrics_updated:
                self.on_metrics_updated(sender_id, metrics)

        elif message.message_type == GossipMessageType.GOSSIP_DIGEST:
            self.stats["anti_entropy_syncs"] += 1
            digest_data = message.payload.get("digest")
            if digest_data:
                remote_digest = GossipDigest.from_dict(digest_data)
                delta = self.compute_anti_entropy_delta(remote_digest)
                if delta:
                    self.local_sequence += 1
                    return GossipMessage(
                        sender_id=self.local_node_id,
                        message_type=GossipMessageType.GOSSIP_DELTA,
                        sequence_number=self.local_sequence,
                        payload={"delta": delta},
                    )

        elif message.message_type == GossipMessageType.GOSSIP_DELTA:
            delta = message.payload.get("delta", {})
            self.apply_anti_entropy_delta(delta)

        elif message.message_type == GossipMessageType.NODE_LEAVE:
            logger.info(f"[{self.local_node_id}] Peer {sender_id} left gracefully")
            sender_peer.health_state = PeerHealthState.DEAD
            if self.on_peer_dropout:
                self.on_peer_dropout(sender_id)

        return None

    async def broadcast_heartbeat(self) -> int:
        """Sends a keepalive ping to all active peers."""
        self.local_sequence += 1
        msg = GossipMessage(
            sender_id=self.local_node_id,
            message_type=GossipMessageType.HEARTBEAT,
            sequence_number=self.local_sequence,
            payload={
                "host": self.host,
                "port": self.port,
                "generation": self.local_generation,
            },
        )
        return await self._send_to_peers(msg, target_peers=self.get_live_peers())

    async def broadcast_metrics(self) -> int:
        """Broadcasts local CPU/RAM to a random subset of peers (fanout)."""
        self.local_sequence += 1
        metrics = self.metrics_collector.collect()
        msg = GossipMessage(
            sender_id=self.local_node_id,
            message_type=GossipMessageType.METRICS_UPDATE,
            sequence_number=self.local_sequence,
            payload={
                "host": self.host,
                "port": self.port,
                "generation": self.local_generation,
                "metrics": metrics.to_dict(),
            },
        )
        targets = self._select_fanout_targets()
        return await self._send_to_peers(msg, target_peers=targets)

    async def initiate_anti_entropy(self) -> int:
        """Sends a digest to a random peer to reconcile state differences."""
        targets = self._select_fanout_targets(count=1)
        if not targets:
            return 0

        digest = self.create_state_digest()
        msg = GossipMessage(
            sender_id=self.local_node_id,
            message_type=GossipMessageType.GOSSIP_DIGEST,
            sequence_number=self.local_sequence,
            payload={"digest": digest.to_dict()},
        )
        return await self._send_to_peers(msg, target_peers=targets)

    def _select_fanout_targets(self, count: Optional[int] = None) -> List[PeerNodeRecord]:
        live_peers = self.get_live_peers()
        if not live_peers:
            return []
        k = count or min(self.fanout, len(live_peers))
        return random.sample(live_peers, min(k, len(live_peers)))

    async def _send_to_peers(self, message: GossipMessage, target_peers: List[PeerNodeRecord]) -> int:
        if not target_peers:
            return 0

        encoded_bytes = message.to_bytes()
        sent_count = 0

        for peer in target_peers:
            try:
                if self.transport_send_fn:
                    await self.transport_send_fn(peer.host, peer.port, encoded_bytes)
                elif self._udp_transport:
                    self._udp_transport.sendto(encoded_bytes, (peer.host, peer.port))
                sent_count += 1
                self.stats["messages_sent"] += 1
            except Exception as e:
                logger.debug(f"Failed to send gossip to {peer.node_id} ({peer.host}:{peer.port}): {e}")

        return sent_count

    # -------------------------------------------------------------------------
    # Failure Detection
    # -------------------------------------------------------------------------

    def check_peer_health(self) -> Tuple[List[str], List[str]]:
        """
        Evaluates elapsed time since last heartbeat for all peers.
        Flags peers as SUSPECT or DEAD.
        """
        now = time.time()
        newly_suspect: List[str] = []
        newly_dead: List[str] = []

        for node_id, peer in self._peers.items():
            elapsed = now - peer.last_heartbeat_time

            if peer.health_state == PeerHealthState.HEALTHY:
                if elapsed >= self.heartbeat_timeout:
                    peer.health_state = PeerHealthState.DEAD
                    peer.consecutive_missed += 1
                    self.stats["dropouts_detected"] += 1
                    newly_dead.append(node_id)
                    logger.warning(
                        f"[{self.local_node_id}] Node {node_id} heartbeat expired ({elapsed:.1f}s > {self.heartbeat_timeout}s). "
                        f"Marked DEAD."
                    )
                    if self.on_peer_dropout:
                        self.on_peer_dropout(node_id)
                elif elapsed >= self.suspect_timeout:
                    peer.health_state = PeerHealthState.SUSPECT
                    peer.consecutive_missed += 1
                    newly_suspect.append(node_id)
                    logger.debug(
                        f"[{self.local_node_id}] Node {node_id} missed heartbeat ({elapsed:.1f}s). Marked SUSPECT."
                    )
                    if self.on_peer_suspect:
                        self.on_peer_suspect(node_id)

            elif peer.health_state == PeerHealthState.SUSPECT:
                if elapsed >= self.heartbeat_timeout:
                    peer.health_state = PeerHealthState.DEAD
                    self.stats["dropouts_detected"] += 1
                    newly_dead.append(node_id)
                    logger.warning(
                        f"[{self.local_node_id}] Suspect node {node_id} exceeded timeout ({elapsed:.1f}s). Marked DEAD."
                    )
                    if self.on_peer_dropout:
                        self.on_peer_dropout(node_id)

        return newly_suspect, newly_dead

    # -------------------------------------------------------------------------
    # Integration with TaskRouter & ReliabilityManager
    # -------------------------------------------------------------------------

    def sync_with_task_router(self, router: Any) -> None:
        """Connects gossip updates to the TaskRouter so it knows peer loads."""
        original_metrics_cb = self.on_metrics_updated
        original_dropout_cb = self.on_peer_dropout

        def _on_metrics(node_id: str, metrics: NodeMetrics) -> None:
            if hasattr(router, "update_worker_metrics"):
                router.update_worker_metrics(
                    node_id=node_id,
                    cpu_percent=metrics.cpu_percent,
                    ram_percent=metrics.ram_percent,
                    active_tasks=metrics.active_tasks,
                )
            if original_metrics_cb:
                original_metrics_cb(node_id, metrics)

        def _on_dropout(dead_node_id: str) -> None:
            if hasattr(router, "mark_worker_dead"):
                router.mark_worker_dead(dead_node_id)
            if original_dropout_cb:
                original_dropout_cb(dead_node_id)

        self.on_metrics_updated = _on_metrics
        self.on_peer_dropout = _on_dropout

    def sync_with_reliability_manager(self, reliability: Any, router: Any) -> None:
        """Connects dropouts to the ReliabilityManager to re-route orphaned tasks."""
        original_dropout_cb = self.on_peer_dropout

        def _on_dropout_recover(dead_node_id: str) -> None:
            logger.warning(
                f"[{self.local_node_id}] Detected worker dropout: {dead_node_id}. "
                f"Triggering task reliability recovery..."
            )
            if hasattr(reliability, "handle_worker_dropout"):
                reassigned = reliability.handle_worker_dropout(dead_node_id, router)
                logger.info(f"[{self.local_node_id}] Reassigned {len(reassigned)} orphaned tasks from {dead_node_id}")
            if original_dropout_cb:
                original_dropout_cb(dead_node_id)

        self.on_peer_dropout = _on_dropout_recover

    # -------------------------------------------------------------------------
    # Background Async Loops
    # -------------------------------------------------------------------------

    async def _gossip_loop(self) -> None:
        """Runs periodic gossip broadcasts every 5 seconds."""
        while self._is_running:
            try:
                await asyncio.sleep(self.gossip_interval)
                if not self._is_running:
                    break

                self.stats["gossip_rounds"] += 1
                await self.broadcast_metrics()

                if self.stats["gossip_rounds"] % 2 == 0:
                    await self.initiate_anti_entropy()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in gossip loop: {e}", exc_info=True)

    async def _health_loop(self) -> None:
        """Runs heartbeat checks every second."""
        while self._is_running:
            try:
                await asyncio.sleep(1.0)
                if not self._is_running:
                    break
                self.check_peer_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitor loop: {e}", exc_info=True)

    async def start(self) -> None:
        """Starts the gossip and health monitoring background tasks."""
        if self._is_running:
            return

        self._is_running = True
        loop = asyncio.get_running_loop()

        self._tasks.append(loop.create_task(self._gossip_loop()))
        self._tasks.append(loop.create_task(self._health_loop()))
        logger.info(
            f"[{self.local_node_id}] GossipMonitor running (interval={self.gossip_interval}s, "
            f"timeout={self.heartbeat_timeout}s, fanout={self.fanout})"
        )

    async def stop(self) -> None:
        """Stops background tasks."""
        if not self._is_running:
            return

        self._is_running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self._udp_transport:
            self._udp_transport.close()
            self._udp_transport = None

        logger.info(f"[{self.local_node_id}] Stopped GossipMonitor")
