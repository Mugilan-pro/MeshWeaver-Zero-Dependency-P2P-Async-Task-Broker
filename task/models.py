"""
MeshWeaver Task Models
Defines core distributed task abstractions, state transitions, and execution result models.
Zero external dependencies - built entirely on the Python standard library.
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class TaskStatus(str, enum.Enum):
    """Lifecycle status states for a MeshWeaver distributed task."""
    PENDING = "PENDING"          # Created, awaiting routing
    SCHEDULED = "SCHEDULED"      # Assigned to a target peer/worker
    RUNNING = "RUNNING"          # Currently executing on worker pool
    COMPLETED = "COMPLETED"      # Execution succeeded with valid return value
    FAILED = "FAILED"            # Execution failed with an unrecoverable exception
    RETRYING = "RETRYING"        # Execution failed or worker dropped; scheduled for retry
    TIMEOUT = "TIMEOUT"          # Task exceeded allowed execution duration
    CANCELLED = "CANCELLED"      # Explicitly cancelled by submitter


class TaskPriority(int, enum.Enum):
    """Task priority levels for queue ordering and scheduling prioritization."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class TaskResult:
    """Represents the execution outcome of a completed, timed-out, or failed task."""
    task_id: str
    status: TaskStatus
    result_payload: Optional[bytes] = None
    return_value: Optional[Any] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    execution_time_seconds: float = 0.0
    executed_by_node_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def success(
        cls,
        task_id: str,
        result_payload: Optional[bytes] = None,
        return_value: Optional[Any] = None,
        execution_time_seconds: float = 0.0,
        executed_by_node_id: Optional[str] = None,
    ) -> TaskResult:
        """Constructs a successful execution outcome."""
        return cls(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            result_payload=result_payload,
            return_value=return_value,
            execution_time_seconds=execution_time_seconds,
            executed_by_node_id=executed_by_node_id,
            timestamp=time.time(),
        )

    @classmethod
    def failure(
        cls,
        task_id: str,
        error_message: str,
        traceback: Optional[str] = None,
        execution_time_seconds: float = 0.0,
        executed_by_node_id: Optional[str] = None,
        is_timeout: bool = False,
    ) -> TaskResult:
        """Constructs a failed or timed-out execution outcome."""
        return cls(
            task_id=task_id,
            status=TaskStatus.TIMEOUT if is_timeout else TaskStatus.FAILED,
            error_message=error_message,
            traceback=traceback,
            execution_time_seconds=execution_time_seconds,
            executed_by_node_id=executed_by_node_id,
            timestamp=time.time(),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Converts result object to a wire-compatible dictionary."""
        return {
            "task_id": self.task_id,
            "status": self.status.value if isinstance(self.status, TaskStatus) else str(self.status),
            "result_payload": self.result_payload.hex() if self.result_payload else None,
            "error_message": self.error_message,
            "traceback": self.traceback,
            "execution_time_seconds": self.execution_time_seconds,
            "executed_by_node_id": self.executed_by_node_id,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TaskResult:
        """Reconstructs result object from wire dictionary."""
        payload = bytes.fromhex(data["result_payload"]) if data.get("result_payload") else None
        return cls(
            task_id=data["task_id"],
            status=TaskStatus(data["status"]),
            result_payload=payload,
            error_message=data.get("error_message"),
            traceback=data.get("traceback"),
            execution_time_seconds=float(data.get("execution_time_seconds", 0.0)),
            executed_by_node_id=data.get("executed_by_node_id"),
            timestamp=float(data.get("timestamp", time.time())),
        )


@dataclass
class Task:
    """
    Core distributed unit of work within the MeshWeaver mesh network.
    Encapsulates compute functions, inputs, lifecycle state, and resilience policies.
    """
    name: str
    payload: bytes
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    origin_node_id: Optional[str] = None
    assigned_node_id: Optional[str] = None
    max_retries: int = 3
    retry_count: int = 0
    timeout_seconds: float = 30.0
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def mark_scheduled(self, node_id: str) -> None:
        """Marks the task as scheduled on a specific target node."""
        self.status = TaskStatus.SCHEDULED
        self.assigned_node_id = node_id
        self.scheduled_at = time.time()

    def mark_running(self) -> None:
        """Marks the task as actively executing."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_completed(self) -> None:
        """Marks the task as successfully completed."""
        self.status = TaskStatus.COMPLETED
        self.completed_at = time.time()

    def mark_failed(self) -> None:
        """Marks the task as terminated with failure."""
        self.status = TaskStatus.FAILED
        self.completed_at = time.time()

    def can_retry(self) -> bool:
        """Validates if retry count is within the bounded max retry limit."""
        return self.retry_count < self.max_retries

    def increment_retry(self) -> None:
        """Advances the retry counter and resets execution timestamps."""
        self.retry_count += 1
        self.status = TaskStatus.RETRYING
        self.assigned_node_id = None
        self.started_at = None

    def to_dict(self) -> Dict[str, Any]:
        """Converts task to a JSON-serializable wire protocol dictionary."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "payload": self.payload.hex() if isinstance(self.payload, bytes) else "",
            "status": self.status.value,
            "priority": self.priority.value,
            "origin_node_id": self.origin_node_id,
            "assigned_node_id": self.assigned_node_id,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Task:
        """Reconstructs task from wire protocol dictionary."""
        payload = bytes.fromhex(data["payload"]) if data.get("payload") else b""
        return cls(
            task_id=data["task_id"],
            name=data["name"],
            payload=payload,
            status=TaskStatus(data.get("status", TaskStatus.PENDING)),
            priority=TaskPriority(data.get("priority", TaskPriority.NORMAL)),
            origin_node_id=data.get("origin_node_id"),
            assigned_node_id=data.get("assigned_node_id"),
            max_retries=int(data.get("max_retries", 3)),
            retry_count=int(data.get("retry_count", 0)),
            timeout_seconds=float(data.get("timeout_seconds", 30.0)),
            created_at=float(data.get("created_at", time.time())),
            scheduled_at=data.get("scheduled_at"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            metadata=data.get("metadata", {}),
        )
