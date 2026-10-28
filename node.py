"""
MeshWeaver Unified Decentralized Node
Coordinates peer discovery (Kademlia DHT), background resource telemetry (Gossip protocol),
load-aware scheduling, non-blocking task execution pools, TLS encrypted networking,
and asymmetric cryptographic signatures.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import config
from dht.node_id import NodeID, NodeContact
from dht.discovery import DHTDiscoveryNode
from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.serializer import TaskSerializer
from task.executor import TaskExecutor
from scheduler.router import TaskRouter, RoutingStrategy, WorkerNodeInfo
from scheduler.reliability import ReliabilityManager
from gossip.telemetry import SystemMetricsCollector, NodeTelemetrySnapshot
from gossip.engine import GossipEngine, TrackedPeer
from security.crypto import NodeKeypair
from security.tls import (
    generate_self_signed_cert,
    create_server_ssl_context,
    create_client_ssl_context,
)
from network.server import TaskServer
from network.client import TaskClient, NetworkConnectionError
from dashboard.cli import MeshDashboard

logger = logging.getLogger("MeshWeaver.Node")


class MeshNode:
    """
    Decentralized P2P Task Broker Node.
    Zero centralized brokers, zero external dependencies required.
    """

    def __init__(
        self,
        host: str = config.DEFAULT_HOST,
        port: int = config.DEFAULT_PORT,
        dht_port: int = config.DHT_DEFAULT_BOOTSTRAP_PORT,
        node_id: Optional[NodeID] = None,
        enable_tls: bool = config.TLS_ENABLED,
        require_signatures: bool = config.REQUIRE_SIGNATURES,
        default_strategy: RoutingStrategy = RoutingStrategy.LOWEST_CPU,
        heartbeat_interval_seconds: float = config.HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_timeout_seconds: float = config.HEARTBEAT_TIMEOUT_SECONDS,
    ) -> None:
        self.host = host
        self.port = port
        self.dht_port = dht_port
        self.node_id = node_id or NodeID.generate()
        self.node_id_hex = self.node_id.hex_value
        self.enable_tls = enable_tls
        self.require_signatures = require_signatures
        self.default_strategy = default_strategy
        self.heartbeat_interval = heartbeat_interval_seconds
        self.heartbeat_timeout = heartbeat_timeout_seconds

        # 1. Security Subsystem (TLS & Ed25519)
        self.keypair = NodeKeypair()
        self.server_ssl_ctx: Optional[ssl.SSLContext] = None
        self.client_ssl_ctx: Optional[ssl.SSLContext] = None
        self._init_tls()

        # 2. Local Compute & Workload Engine
        self.executor = TaskExecutor(node_id=self.node_id_hex)
        self.router = TaskRouter(
            local_node_id=self.node_id_hex,
            default_strategy=self.default_strategy,
            local_cpu_threshold=config.DEFAULT_LOCAL_CPU_THRESHOLD,
        )
        self.reliability = ReliabilityManager(
            default_max_retries=config.DEFAULT_MAX_RETRIES,
            default_lease_timeout=config.DEFAULT_LEASE_TIMEOUT_SECONDS,
        )

        # 3. Kademlia DHT Discovery
        self.dht = DHTDiscoveryNode(
            host=self.host,
            port=self.dht_port,
            node_id=self.node_id,
            k=config.DHT_K_BUCKET_SIZE,
            alpha=config.DHT_ALPHA,
            rpc_timeout=config.DHT_RPC_TIMEOUT_SECONDS,
        )

        # 4. Hardware Resource Metrics
        self.metrics_collector = SystemMetricsCollector(
            node_id=self.node_id_hex,
            host=self.host,
            port=self.port,
        )

        # 5. Network Transport (TCP/TLS Client & Server)
        self.client = TaskClient(
            keypair=self.keypair,
            ssl_context=self.client_ssl_ctx,
            timeout_seconds=config.DEFAULT_TASK_TIMEOUT_SECONDS,
        )
        self.server = TaskServer(
            node_id=self.node_id_hex,
            host=self.host,
            port=self.port,
            executor=self.executor,
            keypair=self.keypair,
            ssl_context=self.server_ssl_ctx,
            require_signatures=self.require_signatures,
        )

        # 6. Gossip & Heartbeat Subsystem
        self.gossip = GossipEngine(
            node_id=self.node_id_hex,
            host=self.host,
            port=self.port,
            router=self.router,
            reliability=self.reliability,
            broadcast_interval_seconds=self.heartbeat_interval,
            heartbeat_timeout_seconds=self.heartbeat_timeout,
            fanout=config.GOSSIP_FANOUT,
            metrics_collector=self.metrics_collector,
            send_gossip_fn=self._send_gossip_to_peer,
        )
        self.server.gossip_engine = self.gossip

        # 7. Terminal Dashboard & Event Logger
        self.dashboard = MeshDashboard()
        self._recent_tasks: List[Dict[str, Any]] = []
        self._is_running = False

        # Hook reliability events into dashboard activity log
        self._setup_event_hooks()

    def _init_tls(self) -> None:
        """Configures TLS contexts for encrypted inter-node communication."""
        if not self.enable_tls:
            return
        try:
            cert_pem, key_pem = generate_self_signed_cert(common_name=f"node-{self.node_id_hex[:8]}")
            self.server_ssl_ctx = create_server_ssl_context(cert_pem, key_pem)
            self.client_ssl_ctx = create_client_ssl_context(verify_peer=False)
            logger.info(f"[{self.node_id_hex[:8]}] TLS/SSL transport layer initialized successfully.")
        except Exception as e:
            logger.warning(f"Could not initialize TLS contexts ({e}). Falling back to unencrypted TCP.")
            self.enable_tls = False
            self.server_ssl_ctx = None
            self.client_ssl_ctx = None

    def _setup_event_hooks(self) -> None:
        """Binds operational event listeners to the dashboard log."""
        def on_reassigned(task: Task, old_worker: str, new_worker: str):
            msg = f"Task {task.name} ({task.task_id[:8]}) re-routed from {old_worker[:8]} -> {new_worker[:8]}"
            self.dashboard.log_event(msg, level="WARN")

        def on_exhausted(task: Task, reason: str):
            msg = f"Task {task.name} ({task.task_id[:8]}) permanently FAILED to dead-letter queue: {reason}"
            self.dashboard.log_event(msg, level="FAIL")

        def on_dropout(dead_node_id: str):
            msg = f"Heartbeat lost for peer {dead_node_id[:8]}! Node flagged DEAD."
            self.dashboard.log_event(msg, level="WARN")

        self.reliability.on_task_reassigned = on_reassigned
        self.reliability.on_task_exhausted = on_exhausted
        self.gossip.on_peer_dropout = on_dropout

    async def _send_gossip_to_peer(self, gossip_data: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Gossip transport callback invoked by GossipEngine."""
        await self.client.send_gossip(gossip_data, target_host=addr[0], target_port=addr[1])

    async def start(self) -> None:
        """Starts all node subsystems asynchronously."""
        if self._is_running:
            return

        # Start TCP/TLS server
        await self.server.start()
        self.port = self.server.port  # update with OS-assigned port if 0 was passed
        self.metrics_collector.port = self.port
        self.gossip.port = self.port

        # Register self in router and gossip as healthy
        self.router.update_worker_metrics(
            node_id=self.node_id_hex,
            cpu_percent=0.0,
            ram_percent=0.0,
            active_tasks=0,
            host=self.host,
            port=self.port,
        )

        # Start Kademlia DHT node
        await self.dht.start()
        self.dht_port = self.dht.port

        # Start Gossip protocol loops
        await self.gossip.start()

        self._is_running = True
        self.dashboard.log_event(f"Node started on TCP:{self.port} | DHT:{self.dht_port}")
        logger.info(
            f"MeshWeaver Node [{self.node_id_hex[:8]}] fully online "
            f"(TCP/TLS:{self.port}, DHT:{self.dht_port}, TLS={self.enable_tls})"
        )

    async def stop(self) -> None:
        """Shuts down all node subsystems cleanly."""
        self._is_running = False
        await self.gossip.stop()
        await self.dht.stop()
        await self.server.stop()
        self.executor.shutdown(wait=False)
        self.dashboard.log_event("Node stopped cleanly.")
        logger.info(f"[{self.node_id_hex[:8]}] MeshWeaver Node stopped.")

    async def bootstrap(self, bootstrap_endpoints: List[Tuple[str, int]]) -> bool:
        """
        Discovers neighbors via Kademlia DHT bootstrap,
        and registers responsive peers into the gossip registry and task router.
        """
        success = await self.dht.bootstrap(bootstrap_endpoints)
        if success:
            # Sync discovered peers into gossip engine
            for contact in self.dht.get_known_peers():
                self.gossip.register_peer(
                    node_id=contact.node_id.hex_value,
                    host=contact.host,
                    port=contact.port,
                )
                self.dashboard.log_event(
                    f"Discovered peer {contact.node_id.hex_value[:8]} via DHT ({contact.host}:{contact.port})"
                )
        return success

    async def submit_task(
        self,
        func: Callable[..., Any],
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        name: str = "distributed_task",
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        strategy: Optional[RoutingStrategy] = None,
    ) -> TaskResult:
        """
        End-to-End Task Submission Workflow:
        1. Package callable into Task with SHA-256 integrity tag.
        2. Evaluate peer CPU metrics and select worker with LOWEST CPU LOAD.
        3. Issue execution lease in ReliabilityManager.
        4. Sign request with local Ed25519 private key.
        5. Transmit over TLS-encrypted TCP connection.
        6. On failure or worker dropout, automatically re-route!
        """
        task = TaskSerializer.package_task(
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            origin_node_id=self.node_id_hex,
        )

        strat = strategy or self.default_strategy
        return await self._route_and_execute_task(task, strat)

    async def submit_packaged_task(
        self,
        task: Task,
        strategy: Optional[RoutingStrategy] = None,
    ) -> TaskResult:
        """Routes and executes a pre-packaged Task instance."""
        task.origin_node_id = self.node_id_hex
        strat = strategy or self.default_strategy
        return await self._route_and_execute_task(task, strat)

    async def _route_and_execute_task(
        self,
        task: Task,
        strategy: RoutingStrategy,
    ) -> TaskResult:
        """Internal dispatch loop with automatic fault recovery and reassignment."""
        excluded_nodes: Set[str] = set()

        while True:
            # 1. Select optimal worker (Lowest CPU)
            chosen_worker = self.router.select_worker(
                task=task,
                strategy=strategy,
                exclude_node_ids=excluded_nodes,
            )

            if not chosen_worker:
                err_msg = f"No eligible workers available across mesh to execute task {task.task_id[:8]}"
                self.dashboard.log_event(err_msg, level="WARN")
                task.mark_failed()
                return TaskResult.failure(
                    task_id=task.task_id,
                    error_message=err_msg,
                    executed_by_node_id=self.node_id_hex,
                )

            # 2. Assign lease
            self.reliability.assign_task(task, chosen_worker.node_id)
            self.dashboard.log_event(
                f"Routed '{task.name}' -> {chosen_worker.node_id[:8]} (CPU: {chosen_worker.cpu_percent:.1f}%)"
            )

            # Record task tracking for UI
            self._update_task_ui_record(task, chosen_worker.node_id)

            # 3. Execute locally or dispatch remotely
            result: Optional[TaskResult] = None
            if chosen_worker.node_id == self.node_id_hex:
                # Local execution
                result = await self.executor.execute(task)
            else:
                # Remote execution over TLS with Ed25519 signature
                try:
                    result = await self.client.send_task(
                        task=task,
                        target_host=chosen_worker.host,
                        target_port=chosen_worker.port,
                        origin_node_id=self.node_id_hex,
                    )
                except NetworkConnectionError as e:
                    logger.warning(f"Connection failed to worker {chosen_worker.node_id[:8]}: {e}")
                    # Peer dropped or unreachable
                    self.gossip.check_heartbeats()
                    result = TaskResult.failure(
                        task_id=task.task_id,
                        error_message=str(e),
                        executed_by_node_id=chosen_worker.node_id,
                    )

            # 4. Check outcome
            if result and result.status == TaskStatus.COMPLETED:
                self.reliability.record_success(task.task_id, result)
                self.dashboard.log_event(
                    f"Task {task.name} COMPLETED by {result.executed_by_node_id[:8]} in {result.execution_time_seconds:.3f}s"
                )
                self._update_task_ui_record(task, result.executed_by_node_id, result)
                return result

            # 5. Handle Failure / Dropout Reassignment
            excluded_nodes.add(chosen_worker.node_id)
            err_reason = result.error_message if result else "Execution failed"
            retry_attempt = self.reliability.record_failure_and_retry(
                task_id=task.task_id,
                reason=err_reason,
                router=self.router,
            )

            if not retry_attempt:
                # Retries exhausted -> Dead-letter queue
                self._update_task_ui_record(task, chosen_worker.node_id, result)
                return result or TaskResult.failure(
                    task_id=task.task_id,
                    error_message=err_reason,
                    executed_by_node_id=chosen_worker.node_id,
                )

            # Task can be retried -> Loop continues with next best worker
            task, new_target = retry_attempt
            self.dashboard.log_event(
                f"Retrying task {task.task_id[:8]} on alternative worker {new_target.node_id[:8]}...",
                level="WARN",
            )

    def _update_task_ui_record(
        self,
        task: Task,
        worker_node_id: str,
        result: Optional[TaskResult] = None,
    ) -> None:
        """Maintains list of recent tasks for dashboard rendering."""
        record = {
            "task_id": task.task_id,
            "name": task.name,
            "status": result.status.value if result else task.status.value,
            "worker": worker_node_id,
            "runtime": result.execution_time_seconds if result else 0.0,
            "retry_count": task.retry_count,
            "max_retries": task.max_retries,
        }
        # Replace existing or append
        for i, existing in enumerate(self._recent_tasks):
            if existing["task_id"] == task.task_id:
                self._recent_tasks[i] = record
                return
        self._recent_tasks.insert(0, record)
        if len(self._recent_tasks) > 10:
            self._recent_tasks.pop()

    def get_dashboard_snapshot(self) -> Dict[str, Any]:
        """Collects current cluster metrics for the Rich dashboard."""
        dht_peers = [
            {
                "node_id": c.node_id.hex_value,
                "host": c.host,
                "port": c.port,
                "xor_distance": self.node_id.xor_distance(c.node_id),
                "rtt_ms": c.rtt_ms,
                "age_seconds": time.time() - c.last_seen,
            }
            for c in self.dht.get_known_peers()
        ]

        cluster_peers = self.gossip.get_cluster_snapshot()
        stats = self.reliability.get_stats()

        task_info = {
            "in_flight": stats["in_flight"],
            "completed": stats["completed"],
            "failed": self.executor.total_failed,
            "retried": sum(t.get("retry_count", 0) for t in self._recent_tasks),
            "dead_letter": stats["dead_letter"],
            "average_execution_time": self.executor.average_execution_time,
            "recent_tasks": list(self._recent_tasks),
        }

        node_info = {
            "node_id": self.node_id_hex,
            "tcp_endpoint": f"{self.host}:{self.port}",
            "dht_endpoint": f"{self.host}:{self.dht_port}",
            "tls_enabled": self.enable_tls,
            "signature_algorithm": self.keypair.algorithm,
            "public_key": self.keypair.public_key_hex,
        }

        return {
            "node_info": node_info,
            "dht_peers": dht_peers,
            "cluster_peers": cluster_peers,
            "task_info": task_info,
        }

    def render_dashboard(self) -> None:
        """Renders the current node state to console."""
        snapshot = self.get_dashboard_snapshot()
        self.dashboard.render_snapshot(snapshot)
