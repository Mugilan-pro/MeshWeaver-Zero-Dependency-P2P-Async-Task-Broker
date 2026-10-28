"""
Decentralized P2P mesh node.
Combines task execution, lowest-CPU routing, gossip telemetry,
fault recovery, and TLS-encrypted communication.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from config import (
    DEFAULT_CLUSTER_SECRET,
    DEFAULT_HOST,
    DEFAULT_PORT,
    ENABLE_TLS,
    HEARTBEAT_INTERVAL_SECONDS,
    HEARTBEAT_TIMEOUT_SECONDS,
    REQUIRE_CRYPTOGRAPHIC_SIGNATURES,
)
from gossip.metrics import SystemMetricsCollector
from gossip.models import GossipMessage, GossipMessageType, NodeMetrics, PeerHealthState
from gossip.monitor import GossipMonitor
from network.protocol import NetworkPacket, ProtocolMessageType
from network.transport import InMemoryMeshRouter, MeshClient, MeshServer
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter, WorkerNodeInfo
from security.crypto import SecurityError, SecurityManager, SignatureVerificationError
from security.tls import (
    create_client_ssl_context,
    create_server_ssl_context,
    ensure_node_certificates,
)
from task.executor import TaskExecutor
from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.serializer import TaskSerializer

logger = logging.getLogger("MeshWeaver.Node")


class MeshNode:
    """
    Decentralized P2P Mesh Node.
    Capable of submitting tasks, routing workloads to the lowest-CPU peer,
    executing compute tasks remotely or locally, detecting peer dropouts,
    and surviving severe network partitions through automatic fault recovery.
    """

    def __init__(
        self,
        node_id: str,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        use_tls: bool = ENABLE_TLS,
        secret_key: bytes = DEFAULT_CLUSTER_SECRET,
        require_signatures: bool = REQUIRE_CRYPTOGRAPHIC_SIGNATURES,
        gossip_interval: float = HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_timeout: float = HEARTBEAT_TIMEOUT_SECONDS,
        shared_router: Optional[InMemoryMeshRouter] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.secret_key = secret_key
        self.require_signatures = require_signatures

        # Optional in-memory network switch for sandboxed unit testing & high-speed simulations
        self.shared_router = shared_router

        # Security Subsystem
        self.security = SecurityManager(
            node_id=node_id,
            secret_key=secret_key,
            require_signatures=require_signatures,
        )

        # Compute & Task Subsystem
        self.executor = TaskExecutor(node_id=node_id)

        # Scheduler & Fault Resilience Subsystem
        self.router = TaskRouter(
            local_node_id=node_id,
            default_strategy=RoutingStrategy.LOWEST_CPU,
        )
        self.reliability = ReliabilityManager()

        # Telemetry Metrics & Gossip Subsystem
        # Provider hook sends live task counts to metrics collector
        def task_counter_provider():
            return (
                self.executor.active_task_count,
                self.executor.total_completed,
                self.executor.total_failed,
            )

        self.metrics_collector = SystemMetricsCollector(
            node_id=node_id,
            task_counter_provider=task_counter_provider,
        )

        self.monitor = GossipMonitor(
            local_node_id=node_id,
            host=host,
            port=port,
            gossip_interval=gossip_interval,
            heartbeat_timeout=heartbeat_timeout,
            metrics_collector=self.metrics_collector,
            transport_send_fn=self._transport_send_gossip,
        )

        # Wire monitor events to scheduler and reliability
        self.monitor.sync_with_task_router(self.router)
        self.monitor.sync_with_reliability_manager(self.reliability, self.router)

        # Networking Layer
        self.server_ssl_ctx: Optional[ssl.SSLContext] = None
        self.client_ssl_ctx: Optional[ssl.SSLContext] = None

        if self.use_tls:
            try:
                cert_file, key_file = ensure_node_certificates(node_id)
                self.server_ssl_ctx = create_server_ssl_context(cert_file, key_file)
                self.client_ssl_ctx = create_client_ssl_context(verify_peer=False)
            except Exception as e:
                logger.warning(f"[{node_id}] TLS initialization failed: {e}. Falling back to plain transport.")
                self.use_tls = False

        self.server = MeshServer(
            node_id=node_id,
            host=host,
            port=port,
            ssl_context=self.server_ssl_ctx,
            handler=self._handle_incoming_packet,
        )
        self.client = MeshClient(
            local_node_id=node_id,
            ssl_context=self.client_ssl_ctx,
        )

        self._is_running = False
        self._lease_task: Optional[asyncio.Task] = None

    @property
    def is_running(self) -> bool:
        return self._is_running

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts the node, networking server, gossip monitor, and lease sweeper."""
        if self._is_running:
            return

        self._is_running = True

        # Register endpoint with shared router if in simulated mode
        if self.shared_router:
            self.shared_router.register_endpoint(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                handler=self._handle_incoming_packet,
            )
        else:
            try:
                await self.server.start()
                self.port = self.server.port
                self.monitor.port = self.port
            except (PermissionError, OSError) as e:
                logger.info(f"[{self.node_id}] Socket start restricted ({e}). Operating in memory mode.")

        # Start gossip telemetry & failure detection
        await self.monitor.start()

        # Start lease timeout background sweeper
        loop = asyncio.get_running_loop()
        self._lease_task = loop.create_task(self._lease_sweep_loop())

        logger.info(
            f"[{self.node_id}] MeshNode fully initialized (Port: {self.port}, "
            f"TLS: {self.use_tls}, Signatures: {self.require_signatures})"
        )

    async def stop(self) -> None:
        """Gracefully shuts down all node subsystems."""
        if not self._is_running:
            return

        self._is_running = False

        if self._lease_task:
            self._lease_task.cancel()
            try:
                await self._lease_task
            except asyncio.CancelledError:
                pass

        if self.shared_router:
            self.shared_router.unregister_endpoint(self.node_id)
        else:
            await self.server.stop()

        await self.monitor.stop()
        self.executor.shutdown(wait=True)
        logger.info(f"[{self.node_id}] MeshNode shutdown complete.")

    # -------------------------------------------------------------------------
    # Peer Management
    # -------------------------------------------------------------------------

    def add_peer(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        is_tls: bool = True,
    ) -> None:
        """Registers a known peer with the node's gossip monitor and task router."""
        peer = self.monitor.add_peer(node_id=node_id, host=host, port=port)
        if peer:
            peer.is_tls = is_tls
        self.router.update_worker_metrics(
            node_id=node_id,
            cpu_percent=0.0,
            host=host,
            port=port,
            is_tls=is_tls,
        )

    # -------------------------------------------------------------------------
    # Workload Submission & Lowest CPU Load Routing (Week 3 Deliverable)
    # -------------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        func: Callable[..., Any],
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        strategy: Optional[RoutingStrategy] = None,
    ) -> TaskResult:
        """
        Submits a compute task to the decentralized mesh.
        1. Packages task and serializes callable with SHA-256 integrity tag.
        2. Routes task to the specific peer with the lowest reported CPU load.
        3. Cryptographically signs task request with HMAC-SHA256 (Week 4).
        4. Transmits over TLS/TCP to worker node and monitors execution lease.
        5. If worker crashes or heartbeat is lost, automatically re-routes to next best peer.
        """
        task = TaskSerializer.package_task(
            name=name,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            origin_node_id=self.node_id,
        )

        return await self._dispatch_task_with_resilience(task, strategy=strategy)

    async def _dispatch_task_with_resilience(
        self,
        task: Task,
        strategy: Optional[RoutingStrategy] = None,
    ) -> TaskResult:
        """
        Orchestrates load-aware routing, dispatch, and automatic fault tolerance retries.
        """
        failed_worker_ids: Set[str] = set()

        while task.can_retry():
            # Week 3: Route task to worker with lowest reported CPU load
            target_worker = self.router.select_worker(
                task,
                strategy=strategy or RoutingStrategy.LOWEST_CPU,
                exclude_node_ids=failed_worker_ids,
            )

            if not target_worker:
                # Fallback to local execution if no healthy remote peers are available
                logger.info(f"[{self.node_id}] No remote worker found for task {task.task_id[:8]}. Executing locally.")
                return await self.executor.execute(task)

            target_id = target_worker.node_id
            self.reliability.assign_task(task, target_id)

            logger.info(
                f"[{self.node_id}] Routing task '{task.name}' ({task.task_id[:8]}) "
                f"to worker {target_id} (CPU: {target_worker.cpu_percent:.1f}%)"
            )

            try:
                if target_id == self.node_id:
                    # Execute locally on local pool
                    result = await self.executor.execute(task)
                else:
                    # Dispatch to remote peer over network
                    result = await self._send_task_to_remote_worker(task, target_worker)

                if result.status == TaskStatus.COMPLETED:
                    self.reliability.record_success(task.task_id, result)
                    return result
                else:
                    # Remote worker returned execution failure
                    reason = result.error_message or "Execution failed on remote worker"
                    failed_worker_ids.add(target_id)
                    retry_decision = self.reliability.record_failure_and_retry(
                        task.task_id, reason, self.router
                    )
                    if not retry_decision:
                        return result

            except Exception as e:
                # Connection dropped, heartbeat lost, or network timeout
                logger.warning(f"[{self.node_id}] Communication failure with worker {target_id}: {e}")
                failed_worker_ids.add(target_id)
                self.router.mark_worker_dead(target_id)
                retry_decision = self.reliability.record_failure_and_retry(
                    task.task_id,
                    f"Connection lost to worker {target_id}: {e}",
                    self.router,
                )
                if not retry_decision:
                    return TaskResult.failure(
                        task_id=task.task_id,
                        error_message=f"All retries exhausted. Last error: {e}",
                        executed_by_node_id=target_id,
                    )

        return TaskResult.failure(
            task_id=task.task_id,
            error_message="Exhausted bounded retry limit",
            executed_by_node_id=self.node_id,
        )

    async def _send_task_to_remote_worker(
        self,
        task: Task,
        worker: WorkerNodeInfo,
    ) -> TaskResult:
        """
        Cryptographically signs and transmits a task request packet to a remote worker.
        """
        # Week 4: Cryptographically sign task request
        if self.require_signatures:
            self.security.sign_task(task)

        packet = NetworkPacket(
            message_type=ProtocolMessageType.TASK_SUBMISSION,
            sender_id=self.node_id,
            payload={"task": task.to_dict()},
            binary_data=task.payload,
            signature=task.signature,
        )

        # Transmit via shared router (simulation) or TLS socket client
        if self.shared_router:
            resp_packet = await self.shared_router.send(worker.host, worker.port, packet)
        else:
            resp_packet = await self.client.send_packet(
                host=worker.host,
                port=worker.port,
                packet=packet,
                wait_for_response=True,
            )

        if not resp_packet:
            raise ConnectionError(f"No response received from worker {worker.node_id}")

        if resp_packet.message_type == ProtocolMessageType.ERROR:
            err_msg = resp_packet.payload.get("error", "Unknown remote error")
            return TaskResult.failure(task_id=task.task_id, error_message=err_msg, executed_by_node_id=worker.node_id)

        # Reconstruct TaskResult from response
        result_dict = resp_packet.payload.get("result", {})
        result = TaskResult.from_dict(result_dict)
        if result.return_value is None and result.result_payload:
            result.return_value = TaskSerializer.deserialize_value(result.result_payload)

        # Week 4: Verify result signature if present
        if self.require_signatures and result.signature:
            self.security.verify_result(result)

        return result

    # -------------------------------------------------------------------------
    # Network Packet Handling (Server Side)
    # -------------------------------------------------------------------------

    async def _handle_incoming_packet(self, packet: NetworkPacket) -> Optional[NetworkPacket]:
        """
        Processes inbound network packets from peer nodes.
        Enforces cryptographic verification for task submissions.
        """
        if packet.message_type == ProtocolMessageType.TASK_SUBMISSION:
            return await self._handle_task_submission(packet)

        if packet.message_type == ProtocolMessageType.HEARTBEAT or packet.message_type == ProtocolMessageType.METRICS_UPDATE:
            # Refresh peer metrics in gossip monitor
            msg = GossipMessage.from_dict(packet.payload)
            await self.monitor.handle_incoming_message(msg)
            return NetworkPacket(
                message_type=ProtocolMessageType.PONG,
                sender_id=self.node_id,
            )

        if packet.message_type == ProtocolMessageType.PING:
            return NetworkPacket(
                message_type=ProtocolMessageType.PONG,
                sender_id=self.node_id,
            )

        return None

    async def _handle_task_submission(self, packet: NetworkPacket) -> NetworkPacket:
        """
        Executes an incoming compute task request:
        1. Unpacks task from wire envelope.
        2. Validates cryptographic signature with HMAC-SHA256 (Week 4).
        3. Executes workload asynchronously in TaskExecutor worker pool.
        4. Signs outcome and returns TASK_RESULT packet.
        """
        task_dict = packet.payload.get("task", {})
        if packet.binary_data and not task_dict.get("payload"):
            task_dict["payload"] = packet.binary_data.hex()

        task = Task.from_dict(task_dict)

        # Week 4: Enforce cryptographic signature verification
        if self.require_signatures:
            try:
                self.security.verify_task(task)
            except SecurityError as e:
                logger.error(f"[{self.node_id}] Rejecting unauthorized task request: {e}")
                return NetworkPacket(
                    message_type=ProtocolMessageType.ERROR,
                    sender_id=self.node_id,
                    payload={"error": f"Security verification failed: {e}", "task_id": task.task_id},
                )

        # Execute task locally
        result = await self.executor.execute(task)

        # Week 4: Cryptographically sign result
        if self.require_signatures:
            self.security.sign_result(result)

        return NetworkPacket(
            message_type=ProtocolMessageType.TASK_RESULT,
            sender_id=self.node_id,
            payload={"result": result.to_dict()},
            binary_data=result.result_payload,
            signature=result.signature,
        )

    async def _transport_send_gossip(self, host: str, port: int, data: bytes) -> None:
        """Gossip transport callback."""
        msg = GossipMessage.from_bytes(data)
        pkt = NetworkPacket(
            message_type=ProtocolMessageType.METRICS_UPDATE,
            sender_id=self.node_id,
            payload=msg.to_dict(),
        )
        if self.shared_router:
            try:
                await self.shared_router.send(host, port, pkt)
            except Exception:
                pass
        else:
            try:
                await self.client.send_packet(host, port, pkt, wait_for_response=False)
            except Exception:
                pass

    async def _lease_sweep_loop(self) -> None:
        """Periodic background sweep for expired task leases."""
        while self._is_running:
            try:
                await asyncio.sleep(2.0)
                if not self._is_running:
                    break
                expired = self.reliability.check_lease_timeouts(self.router)
                if expired:
                    logger.warning(f"[{self.node_id}] Re-routed {len(expired)} expired task leases")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.node_id}] Lease sweeper error: {e}")
