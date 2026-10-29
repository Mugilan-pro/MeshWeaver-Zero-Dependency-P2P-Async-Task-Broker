"""
MeshWeaver Asynchronous TCP/TLS Task Client
Dispatches signed compute workloads to remote mesh workers, enforces execution timeouts,
and handles network communication errors.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any, Dict, Optional, Tuple

from task.models import Task, TaskResult, TaskStatus
from task.serializer import TaskSerializer
from security.crypto import NodeKeypair
from .protocol import MessageType, read_frame, write_frame, ProtocolError

logger = logging.getLogger("MeshWeaver.Network.Client")


class NetworkConnectionError(Exception):
    """Raised when connection to a target peer node fails or is refused."""
    pass


class TaskClient:
    """
    Client for transmitting signed tasks and gossip metrics to remote peers.
    Supports transparent TLS/SSL socket encryption.
    """

    def __init__(
        self,
        keypair: Optional[NodeKeypair] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.keypair = keypair or NodeKeypair()
        self.ssl_context = ssl_context
        self.timeout_seconds = timeout_seconds

    async def send_task(
        self,
        task: Task,
        target_host: str,
        target_port: int,
        origin_node_id: str,
        timeout_seconds: Optional[float] = None,
    ) -> TaskResult:
        """
        Signs and transmits a Task over TCP/TLS to target worker node.
        Awaits and returns the TaskResult.
        """
        timeout = timeout_seconds or task.timeout_seconds or self.timeout_seconds

        # 1. Generate cryptographic signature metadata
        auth_metadata = self.keypair.sign_task_request(
            task_id=task.task_id,
            payload_bytes=task.payload,
            origin_node_id=origin_node_id,
        )

        request_frame = {
            "type": MessageType.TASK_EXECUTE_REQUEST,
            "task_id": task.task_id,
            "name": task.name,
            "origin_node_id": origin_node_id,
            "payload_hex": task.payload.hex(),
            "priority": task.priority.value,
            "timeout_seconds": task.timeout_seconds,
            "max_retries": task.max_retries,
            "auth": auth_metadata,
        }

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=target_host,
                    port=target_port,
                    ssl=self.ssl_context,
                ),
                timeout=5.0,
            )
        except Exception as e:
            logger.warning(f"Could not connect to peer {target_host}:{target_port}: {e}")
            raise NetworkConnectionError(f"Connection failed to {target_host}:{target_port}: {e}") from e

        try:
            # 2. Write request frame
            await write_frame(writer, request_frame)

            # 3. Read response frame
            response_frame = await asyncio.wait_for(
                read_frame(reader),
                timeout=timeout + 5.0,
            )

            if response_frame is None:
                raise NetworkConnectionError("Peer closed connection before returning task result")

            status_str = response_frame.get("status", TaskStatus.FAILED.value)
            status = TaskStatus(status_str)
            raw_result_hex = response_frame.get("result_payload_hex")
            result_payload = bytes.fromhex(raw_result_hex) if raw_result_hex else b""
            return_val = TaskSerializer.deserialize_value(result_payload) if result_payload else None

            return TaskResult(
                task_id=task.task_id,
                status=status,
                result_payload=result_payload,
                return_value=return_val,
                execution_time_seconds=float(response_frame.get("execution_time_seconds", 0.0)),
                error_message=response_frame.get("error_message"),
                traceback=response_frame.get("traceback"),
                executed_by_node_id=response_frame.get("executed_by_node_id", "remote_node"),
            )

        except asyncio.TimeoutError:
            return TaskResult.failure(
                task_id=task.task_id,
                error_message=f"Network task execution timed out after {timeout:.1f}s",
                executed_by_node_id=f"{target_host}:{target_port}",
                is_timeout=True,
            )
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def send_gossip(
        self,
        gossip_data: Dict[str, Any],
        target_host: str,
        target_port: int,
    ) -> bool:
        """Sends a telemetry snapshot to a peer node."""
        frame = {
            "type": MessageType.GOSSIP_SNAPSHOT,
            "data": gossip_data,
        }
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=target_host,
                    port=target_port,
                    ssl=self.ssl_context,
                ),
                timeout=3.0,
            )
            await write_frame(writer, frame)
            resp = await asyncio.wait_for(read_frame(reader), timeout=3.0)
            writer.close()
            await writer.wait_closed()
            return resp is not None and resp.get("status") == "OK"
        except Exception as e:
            logger.debug(f"Failed to dispatch gossip to {target_host}:{target_port}: {e}")
            return False

    async def ping_node(
        self,
        target_host: str,
        target_port: int,
    ) -> Tuple[bool, Optional[str]]:
        """Pings a node over TCP/TLS. Returns (is_alive, remote_node_id)."""
        frame = {"type": MessageType.PING}
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=target_host,
                    port=target_port,
                    ssl=self.ssl_context,
                ),
                timeout=2.0,
            )
            await write_frame(writer, frame)
            resp = await asyncio.wait_for(read_frame(reader), timeout=2.0)
            writer.close()
            await writer.wait_closed()
            if resp and resp.get("type") == MessageType.PONG:
                return True, resp.get("responder_node_id")
            return False, None
        except Exception:
            return False, None
