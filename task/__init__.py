"""
MeshWeaver Task Engine
Asynchronous task abstraction, high-fidelity serialization, non-blocking execution,
and cryptographic security verification.
Part of the MeshWeaver Decentralized Task Broker.
"""

from .models import (
    Task,
    TaskResult,
    TaskStatus,
    TaskPriority,
)
from .serializer import (
    TaskSerializer,
    serialize_payload,
    deserialize_payload,
    SerializationError,
    IntegrityError,
)
from .executor import (
    TaskExecutor,
)
from .security import (
    TaskSigner,
    TaskVerifier,
    SecurityError,
    MissingSignatureError,
    UntrustedNodeError,
    InvalidSignatureError,
    sign_task,
    verify_task,
)

__all__ = [
    "Task",
    "TaskResult",
    "TaskStatus",
    "TaskPriority",
    "TaskSerializer",
    "serialize_payload",
    "deserialize_payload",
    "SerializationError",
    "IntegrityError",
    "TaskExecutor",
    "TaskSigner",
    "TaskVerifier",
    "SecurityError",
    "MissingSignatureError",
    "UntrustedNodeError",
    "InvalidSignatureError",
    "sign_task",
    "verify_task",
]
