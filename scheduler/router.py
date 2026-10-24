"""
MeshWeaver Workload Router
Selects target worker peers across the decentralized mesh network
using load-aware balancing algorithms (lowest CPU load, composite score, etc.).
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
    cpu_percent: float = 0.0          # 0.0 to 100.0%
    ram_percent: float = 0.0          # 0.0 to 100.0%
    active_tasks: int = 0             # Number of currently running tasks
    is_alive: bool = True
    last_heartbeat: float = field(default_factory=time.time)
    heartbeat_timeout_seconds: float = 15.0

    @property
    def is_healthy(self) -> bool:
        """Determines if the worker node is alive and its heartbeat has not expired."""
        if not self.is_alive:
            return False
        return (time.time() - self.last_heartbeat) <= self.heartbeat_timeout_seconds

    @property
    def composite_load_score(self) -> float:
        """
        Calculates a composite load index:
        60% CPU utilization + 30% active task pressure + 10% RAM utilization.
        Lower score indicates greater available capacity.
        """
        task_pressure = min(self.active_tasks * 10.0, 100.0)
        return (self.cpu_percent * 0.6) + (task_pressure * 0.3) + (self.ram_percent * 0.1)

    def update_metrics(
        self,
        cpu_percent: float,
        ram_percent: float,
        active_tasks: int,
    ) -> None:
        """Updates telemetry metrics and refreshes the heartbeat timestamp."""
        self.cpu_percent = max(0.0, min(100.0, float(cpu_percent)))
        self.ram_percent = max(0.0, min(100.0, float(ram_percent)))
        self.active_tasks = max(0, int(active_tasks))
        self.is_alive = True
        self.last_heartbeat = time.time()


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
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> None:
        """Convenience updater for incoming gossip telemetry snapshots."""
        if node_id not in self._workers:
            self._workers[node_id] = WorkerNodeInfo(
                node_id=node_id,
                host=host,
                port=port,
            )
        self._workers[node_id].update_metrics(cpu_percent, ram_percent, active_tasks)

    def mark_worker_dead(self, node_id: str) -> None:
        """Flags a peer node as unresponsive or dead."""
        if node_id in self._workers:
            self._workers[node_id].is_alive = False
            logger.warning(f"Worker {node_id} flagged DEAD in TaskRouter")

    def remove_worker(self, node_id: str) -> None:
        """Removes a peer node completely from the routing registry."""
        self._workers.pop(node_id, None)

    def get_healthy_workers(self, exclude_node_ids: Optional[Set[str]] = None) -> List[WorkerNodeInfo]:
        """Returns candidate peer nodes that are alive, responsive, and not excluded."""
        exclude = exclude_node_ids or set()
        return [
            w for w in self._workers.values()
            if w.is_healthy and w.node_id not in exclude
        ]

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
            if local and local.cpu_percent <= self.local_cpu_threshold:
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
