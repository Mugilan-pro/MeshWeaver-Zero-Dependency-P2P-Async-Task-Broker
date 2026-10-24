"""
Gossip and node health data structures for MeshWeaver.
Defines node metrics, peer status, and message payloads exchanged across peers.
"""

from __future__ import annotations

import enum
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class PeerHealthState(str, enum.Enum):
    """Status of a peer in the network."""
    HEALTHY = "HEALTHY"    # Node is responding normally
    SUSPECT = "SUSPECT"    # Missed a heartbeat, waiting to see if it responds
    DEAD = "DEAD"          # Timed out completely, marked dead


class GossipMessageType(str, enum.Enum):
    """Types of messages sent over gossip."""
    HEARTBEAT = "HEARTBEAT"            # Simple ping/keepalive
    METRICS_UPDATE = "METRICS_UPDATE"  # CPU, RAM, and task stats update
    GOSSIP_DIGEST = "GOSSIP_DIGEST"    # Version summary sent to find who has newer info
    GOSSIP_DELTA = "GOSSIP_DELTA"      # Newer info sent back in response to a digest
    NODE_LEAVE = "NODE_LEAVE"          # Graceful shutdown notification


@dataclass
class NodeMetrics:
    """Hardware and workload statistics for a node."""
    node_id: str
    cpu_percent: float = 0.0             # CPU usage (0.0 to 100.0)
    ram_percent: float = 0.0             # RAM usage (0.0 to 100.0)
    memory_used_mb: float = 0.0          # RAM used in MB
    memory_total_mb: float = 0.0         # Total RAM in MB
    active_tasks: int = 0                # Currently running tasks
    completed_tasks: int = 0             # Total finished tasks
    failed_tasks: int = 0                # Total failed tasks
    load_average_1m: float = 0.0         # 1-minute load average
    uptime_seconds: float = 0.0          # Node uptime in seconds
    timestamp: float = field(default_factory=time.time)
    sequence_number: int = 0             # Incrementing counter for updates

    @property
    def composite_score(self) -> float:
        """
        Calculates a load score:
        60% CPU + 30% task pressure + 10% RAM.
        Lower score means the node is less busy.
        """
        task_pressure = min(self.active_tasks * 10.0, 100.0)
        return (self.cpu_percent * 0.6) + (task_pressure * 0.3) + (self.ram_percent * 0.1)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> NodeMetrics:
        filtered = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**filtered)


@dataclass
class PeerNodeRecord:
    """Tracks a known peer's address, latest metrics, and health state."""
    node_id: str
    host: str = "127.0.0.1"
    port: int = 8000
    metrics: NodeMetrics = field(default_factory=lambda: NodeMetrics(node_id="unknown"))
    health_state: PeerHealthState = PeerHealthState.HEALTHY
    last_heartbeat_time: float = field(default_factory=time.time)
    heartbeat_sequence: int = 0
    generation: int = 0                  # Changes when node restarts to discard old state
    consecutive_missed: int = 0
    round_trip_time_ms: float = 0.0

    @property
    def is_alive(self) -> bool:
        return self.health_state != PeerHealthState.DEAD

    @property
    def is_healthy(self) -> bool:
        return self.health_state == PeerHealthState.HEALTHY

    @property
    def is_suspect(self) -> bool:
        return self.health_state == PeerHealthState.SUSPECT

    @property
    def is_dead(self) -> bool:
        return self.health_state == PeerHealthState.DEAD

    @property
    def seconds_since_last_heartbeat(self) -> float:
        return max(0.0, time.time() - self.last_heartbeat_time)

    def record_heartbeat(
        self,
        sequence: int = 0,
        generation: int = 0,
        metrics: Optional[NodeMetrics] = None,
        rtt_ms: float = 0.0,
    ) -> None:
        """Call this when a valid message arrives from this peer."""
        self.last_heartbeat_time = time.time()
        self.consecutive_missed = 0
        self.health_state = PeerHealthState.HEALTHY
        if sequence > self.heartbeat_sequence or generation > self.generation:
            self.heartbeat_sequence = sequence
            self.generation = generation
        if metrics is not None:
            self.metrics = metrics
        if rtt_ms > 0:
            self.round_trip_time_ms = rtt_ms

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "metrics": self.metrics.to_dict(),
            "health_state": self.health_state.value,
            "last_heartbeat_time": self.last_heartbeat_time,
            "heartbeat_sequence": self.heartbeat_sequence,
            "generation": self.generation,
            "consecutive_missed": self.consecutive_missed,
            "round_trip_time_ms": self.round_trip_time_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PeerNodeRecord:
        metrics_data = data.get("metrics")
        metrics = NodeMetrics.from_dict(metrics_data) if isinstance(metrics_data, dict) else NodeMetrics(node_id=data.get("node_id", "unknown"))
        health_state = PeerHealthState(data.get("health_state", PeerHealthState.HEALTHY.value))
        return cls(
            node_id=data["node_id"],
            host=data.get("host", "127.0.0.1"),
            port=data.get("port", 8000),
            metrics=metrics,
            health_state=health_state,
            last_heartbeat_time=data.get("last_heartbeat_time", time.time()),
            heartbeat_sequence=data.get("heartbeat_sequence", 0),
            generation=data.get("generation", 0),
            consecutive_missed=data.get("consecutive_missed", 0),
            round_trip_time_ms=data.get("round_trip_time_ms", 0.0),
        )


@dataclass
class GossipDigest:
    """Summary of node versions for anti-entropy sync (node_id -> generation, sequence, timestamp)."""
    sender_id: str
    entries: Dict[str, Tuple[int, int, float]] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "entries": self.entries,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> GossipDigest:
        raw_entries = data.get("entries", {})
        entries = {k: tuple(v) for k, v in raw_entries.items()}
        return cls(
            sender_id=data["sender_id"],
            entries=entries,
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class GossipMessage:
    """Network message envelope for gossip and heartbeats."""
    sender_id: str
    message_type: GossipMessageType
    sequence_number: int = 0
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    signature: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sender_id": self.sender_id,
            "message_type": self.message_type.value,
            "sequence_number": self.sequence_number,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> GossipMessage:
        return cls(
            sender_id=data["sender_id"],
            message_type=GossipMessageType(data["message_type"]),
            sequence_number=data.get("sequence_number", 0),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", time.time()),
            signature=data.get("signature"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> GossipMessage:
        data = json.loads(json_str)
        return cls.from_dict(data)

    def to_bytes(self) -> bytes:
        return self.to_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw_bytes: bytes) -> GossipMessage:
        return cls.from_json(raw_bytes.decode("utf-8"))
