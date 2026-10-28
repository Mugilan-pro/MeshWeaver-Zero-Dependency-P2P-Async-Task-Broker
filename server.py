"""
MeshWeaver Asynchronous TCP/TLS Task Server
Listens for remote compute workload requests, validates cryptographic signatures,
executes tasks asynchronously in the worker pool, and returns signed results.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any, Callable, Dict, Optional

from task.models import Task, TaskPriority, TaskResult, TaskStatus
from task.executor import TaskExecutor
from security.crypto import NodeKeypair, SecurityError, SignatureVerificationError
from .protocol import MessageType, read_frame, write_frame

logger = logging.getLogger("MeshWeaver.Network.Server")


class TaskServer:
    """
    Asynchronous TCP/TLS Server for distributed task execution.
    Listens for authenticated compute requests from peer mesh nodes.
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 8000,
        executor: Optional[TaskExecutor] = None,
        keypair: Optional[NodeKeypair] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        require_signatures: bool = True,
        gossip_engine: Optional[Any] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.executor = executor or TaskExecutor(node_id=node_id)
        self.keypair = keypair or NodeKeypair()
        self.ssl_context = ssl_context
        self.require_signatures = require_signatures
        self.gossip_engine = gossip_engine

        self._server: Optional[asyncio.Server] = None
        self._active_writers: set = set()
        self._is_running = False

        # Metrics for dashboard observability
        self.requests_received = 0
        self.signatures_rejected = 0
        self.tasks_executed = 0

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def endpoint(self) -> tuple[str, int]:
        return self.host, self.port

    @property
    def is_tls_enabled(self) -> bool:
        return self.ssl_context is not None

    async def start(self) -> None:
        """Starts the TCP/TLS listener."""
        if self._is_running:
            return

        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.host,
            port=self.port,
            ssl=self.ssl_context,
        )

        # Retrieve actual bound port if port 0 was passed
        sockets = self._server.sockets
        if sockets:
            actual_addr = sockets[0].getsockname()
            self.host = actual_addr[0]
            self.port = actual_addr[1]

        self._is_running = True
        proto = "TLS" if self.ssl_context else "TCP"
        logger.info(f"[{self.node_id}] TaskServer listening on {proto} {self.host}:{self.port}")

    async def stop(self) -> None:
        """Stops the TCP server."""
        self._is_running = False
        for w in list(self._active_writers):
            try:
                w.close()
            except Exception:
                pass
        self._active_writers.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info(f"[{self.node_id}] TaskServer stopped.")

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Dispatches incoming framed messages over a persistent TCP/TLS stream."""
        peer_addr = writer.get_extra_info("peername")
        self._active_writers.add(writer)
        try:
            while self._is_running:
                frame = await read_frame(reader)
                if frame is None:
                    break

                self.requests_received += 1
                msg_type = frame.get("type")

                if msg_type == MessageType.TASK_EXECUTE_REQUEST:
                    response = await self._process_task_request(frame)
                    await write_frame(writer, response)

                elif msg_type == MessageType.GOSSIP_SNAPSHOT:
                    if self.gossip_engine:
                        self.gossip_engine.handle_incoming_gossip(frame.get("data", {}))
                    await write_frame(writer, {"type": MessageType.GOSSIP_ACK, "status": "OK"})

                elif msg_type == MessageType.PING:
                    await write_frame(writer, {
                        "type": MessageType.PONG,
                        "responder_node_id": self.node_id,
                    })

                else:
                    await write_frame(writer, {
                        "type": "ERROR",
                        "error": f"Unknown message type: {msg_type}",
                    })

        except Exception as e:
            logger.debug(f"[{self.node_id}] Connection error with {peer_addr}: {e}")
        finally:
            self._active_writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _process_task_request(self, frame: Dict[str, Any]) -> Dict[str, Any]:
        """Validates signatures and executes the task workload."""
        task_id = frame.get("task_id", "")
        name = frame.get("name", "unnamed_task")
        origin_node_id = frame.get("origin_node_id", "unknown")
        payload_hex = frame.get("payload_hex", "")
        priority_val = frame.get("priority", TaskPriority.NORMAL.value)
        timeout_seconds = float(frame.get("timeout_seconds", 30.0))
        max_retries = int(frame.get("max_retries", 3))
        auth_metadata = frame.get("auth", {})

        try:
            payload_bytes = bytes.fromhex(payload_hex)
        except Exception:
            return {
                "type": MessageType.TASK_EXECUTE_RESPONSE,
                "task_id": task_id,
                "status": TaskStatus.FAILED.value,
                "error_message": "Invalid hexadecimal task payload encoding",
                "executed_by_node_id": self.node_id,
            }

        # 1. Cryptographic Signature Verification
        if self.require_signatures:
            try:
                self.keypair.verify_task_request(
                    task_id=task_id,
                    payload_bytes=payload_bytes,
                    origin_node_id=origin_node_id,
                    auth_metadata=auth_metadata,
                )
            except SecurityError as e:
                self.signatures_rejected += 1
                logger.warning(f"[{self.node_id}] SECURITY REJECTION: {e}")
                return {
                    "type": MessageType.TASK_EXECUTE_RESPONSE,
                    "task_id": task_id,
                    "status": TaskStatus.FAILED.value,
                    "error_message": f"Security Verification Failed: {e}",
                    "executed_by_node_id": self.node_id,
                    "is_security_failure": True,
                }

        # 2. Reconstruct Task and execute in worker pool
        task = Task(
            name=name,
            payload=payload_bytes,
            task_id=task_id,
            priority=TaskPriority(priority_val),
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            origin_node_id=origin_node_id,
        )

        self.tasks_executed += 1
        result = await self.executor.execute(task)

        # 3. Format signed response
        result_hex = result.result_payload.hex() if result.result_payload else None
        response = {
            "type": MessageType.TASK_EXECUTE_RESPONSE,
            "task_id": task_id,
            "status": result.status.value,
            "result_payload_hex": result_hex,
            "error_message": result.error_message,
            "traceback": result.traceback,
            "execution_time_seconds": result.execution_time_seconds,
            "executed_by_node_id": self.node_id,
        }

        # Sign the response payload
        sig_data = f"{task_id}:{result.status.value}:{result.execution_time_seconds:.4f}".encode("utf-8")
        response["signature"] = self.keypair.sign(sig_data)

        return response
