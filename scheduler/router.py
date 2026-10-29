"""
Task router for load-aware scheduling.
Selects target workers using lowest-CPU, composite load, or round-robin strategies.
"""

from __future__ import annotations

import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from task.models import Task

logger = logging.getLogger("MeshWeaver.TaskRouter")


class RoutingStrategy(str, enum.Enum):
    """Supported workload distribution strategies."""
    LOWEST_CPU = "LOWEST_CPU"            # Routes to node with lowest reported CPU usage
    COMPOSITE_LOAD = "COMPOSITE_LOAD"    # Weighted composite of CPU, RAM, and active task pressure
    ROUND_ROBIN = "ROUND_ROBIN"          # Cyclical rotation across live healthy nodes
    LOCAL_FIRST = "LOCAL_FIRST"          # Executes locally if local CPU is low, otherwise offloads
    RANDOM = "RANDOM"                    # Uniform random choice among healthy nodes


@dataclass
class WorkerNodeInfo:
    """Represents a candidate peer node and its runtime health metrics."""
    node_id: str
    host: str = "127.0.0.1"
    port: int = 8000
    cpu_percent: float = 0.0              # 0.0 to 100.0%
    ram_percent: float = 0.0              # 0.0 to 100.0%
    active_tasks: int = 0                 # Number of currently running tasks
    max_task_capacity: int = 16           # Max concurrent tasks allowed on this node
    latency_ms: float = 0.0               # Estimated round-trip latency to this peer
    is_alive: bool = True
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_timeout_seconds: float = 15.0
    is_tls: bool = True
    round_trip_time_ms: float = 0.0

    @property
    def is_healthy(self) -> bool:
        """Determines if the worker node is alive and its heartbeat has not expired."""
        if not self.is_alive:
            return False
        return (time.time() - self.last_heartbeat) <= self.heartbeat_timeout_seconds

    @property
    def is_overloaded(self) -> bool:
        """Indicates if worker has reached task capacity or critical CPU threshold (>95%)."""
        return self.active_tasks >= self.max_task_capacity or self.cpu_percent >= 95.0

    @property
    def composite_load_score(self) -> float:
        """
        Calculates a composite load index:
        60% CPU utilization + 30% active task pressure + 10% RAM utilization.
        Lower score indicates greater available capacity.
        """
        task_pressure = min((self.active_tasks / max(1, self.max_task_capacity)) * 100.0, 100.0)
        return round((self.cpu_percent * 0.6) + (task_pressure * 0.3) + (self.ram_percent * 0.1), 2)

    def update_metrics(
        self,
        cpu_percent: float,
        ram_percent: float,
        active_tasks: int,
        latency_ms: float = 0.0,
    ) -> None:
        """Updates telemetry metrics and refreshes the heartbeat timestamp."""
        self.cpu_percent = max(0.0, min(100.0, float(cpu_percent)))
        self.ram_percent = max(0.0, min(100.0, float(ram_percent)))
        self.active_tasks = max(0, int(active_tasks))
        self.latency_ms = max(0.0, float(latency_ms))
        self.is_alive = True
        self.last_heartbeat = time.time()

    def to_telemetry_dict(self) -> Dict[str, object]:
        """Serializes node metrics for Gossip packet transmission."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "cpu_percent": self.cpu_percent,
            "ram_percent": self.ram_percent,
            "active_tasks": self.active_tasks,
            "load_score": self.composite_load_score,
            "is_alive": self.is_alive,
            "last_heartbeat": self.last_heartbeat,
        }


class TaskRouter:
    """
    Decentralized Task Router.
    Evaluates peer metrics collected via Gossip and DHT routing tables
    to match compute tasks with the most capable worker node.
    """

    def __init__(
        self,
        local_node_id: str,
        default_strategy: RoutingStrategy = RoutingStrategy.LOWEST_CPU,
        local_cpu_threshold: float = 50.0,
    ) -> None:
        self.local_node_id = local_node_id
        self.strategy = default_strategy
        self.local_cpu_threshold = local_cpu_threshold

        # Peer node registry: node_id -> WorkerNodeInfo
        self._workers: Dict[str, WorkerNodeInfo] = {}
        self._round_robin_index = 0

    def register_or_update_worker(self, worker_info: WorkerNodeInfo) -> None:
        """Registers or refreshes a peer worker node."""
        self._workers[worker_info.node_id] = worker_info

    def update_worker_metrics(
        self,
        node_id: str,
        cpu_percent: float,
        ram_percent: float = 0.0,
        active_tasks: int = 0,
        latency_ms: float = 0.0,
        host: str = "127.0.0.1",
        port: int = 8000,
        is_tls: bool = True,
        round_trip_time_ms: float = 0.0,
    ) -> None:
        """Convenience updater for incoming gossip telemetry snapshots."""
        if node_id not in self._workers:
            self._workers[node_id] = WorkerNodeInfo(
                node_id=node_id,
                host=host,
                port=port,
                is_tls=is_tls,
                round_trip_time_ms=round_trip_time_ms,
            )
        else:
            self._workers[node_id].host = host
            self._workers[node_id].port = port
            self._workers[node_id].is_tls = is_tls
            if round_trip_time_ms > 0:
                self._workers[node_id].round_trip_time_ms = round_trip_time_ms
        self._workers[node_id].update_metrics(cpu_percent, ram_percent, active_tasks)
        self._workers[node_id].update_metrics(cpu_percent, ram_percent, active_tasks, latency_ms)

    def mark_worker_dead(self, node_id: str) -> None:
        """Flags a peer node as unresponsive or dead."""
        if node_id in self._workers:
            self._workers[node_id].is_alive = False
            logger.warning(f"Worker {node_id} flagged DEAD in TaskRouter")

    def remove_worker(self, node_id: str) -> None:
        """Removes a peer node completely from the routing registry."""
        self._workers.pop(node_id, None)

    def get_healthy_workers(
        self,
        exclude_node_ids: Optional[Set[str]] = None,
        allow_overloaded: bool = False,
    ) -> List[WorkerNodeInfo]:
        """Returns candidate peer nodes that are alive, responsive, and available."""
        exclude = exclude_node_ids or set()
        candidates = [
            w for w in self._workers.values()
            if w.is_healthy and w.node_id not in exclude
        ]
        if not allow_overloaded:
            non_overloaded = [w for w in candidates if not w.is_overloaded]
            if non_overloaded:
                return non_overloaded
        return candidates

    def get_worker_rankings(self) -> List[WorkerNodeInfo]:
        """Returns all registered healthy workers sorted by composite load score ascending."""
        healthy = self.get_healthy_workers(allow_overloaded=True)
        return sorted(healthy, key=lambda w: w.composite_load_score)

    def select_worker(
        self,
        task: Task,
        strategy: Optional[RoutingStrategy] = None,
        exclude_node_ids: Optional[Set[str]] = None,
    ) -> Optional[WorkerNodeInfo]:
        """
        Selects the optimal worker node for task execution.
        Guarantees dead nodes and explicitly excluded nodes are filtered out.
        """
        strat = strategy or self.strategy
        candidates = self.get_healthy_workers(exclude_node_ids=exclude_node_ids)

        if not candidates:
            logger.warning(f"No available healthy workers to route task {task.task_id[:8]}")
            return None

        # 1. LOCAL_FIRST: Keep execution local if capacity allows
        if strat == RoutingStrategy.LOCAL_FIRST:
            local = next((w for w in candidates if w.node_id == self.local_node_id), None)
            if local and local.cpu_percent <= self.local_cpu_threshold and not local.is_overloaded:
                logger.info(f"Retained task {task.task_id[:8]} locally on {self.local_node_id} (CPU: {local.cpu_percent}%)")
                return local
            strat = RoutingStrategy.LOWEST_CPU

        # 2. LOWEST_CPU: Route to node with the lowest CPU utilization
        if strat == RoutingStrategy.LOWEST_CPU:
            chosen = min(candidates, key=lambda w: (w.cpu_percent, w.active_tasks))
            logger.info(
                f"Routed task {task.task_id[:8]} to worker {chosen.node_id} (CPU: {chosen.cpu_percent:.1f}%)"
            )
            return chosen

        # 3. COMPOSITE_LOAD: Weighted multi-variable score
        if strat == RoutingStrategy.COMPOSITE_LOAD:
            chosen = min(candidates, key=lambda w: w.composite_load_score)
            logger.info(
                f"Routed task {task.task_id[:8]} to worker {chosen.node_id} (Load Score: {chosen.composite_load_score:.1f})"
            )
            return chosen

        # 4. ROUND_ROBIN
        if strat == RoutingStrategy.ROUND_ROBIN:
            self._round_robin_index = (self._round_robin_index + 1) % len(candidates)
            return candidates[self._round_robin_index]

        return candidates[0]
