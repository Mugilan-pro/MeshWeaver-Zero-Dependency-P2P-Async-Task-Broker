"""
MeshWeaver Task Serialization Engine
Handles encoding, binary packaging, and cryptographically verified deserialization
of distributed Python workloads over the peer-to-peer mesh.

Supports `cloudpickle` for dynamic lambdas/closures with automatic zero-dependency
fallback to the Python standard library `pickle` and `json`.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import pickle
import random
import time
from typing import Any, Callable, Dict, Optional, Tuple

from .models import Task, TaskPriority

# Dynamic dependency resolution: prefer cloudpickle, fallback to stdlib
try:
    import cloudpickle
    _HAS_CLOUDPICKLE = True
except ImportError:
    cloudpickle = None
    _HAS_CLOUDPICKLE = False

MAGIC_HEADER_CLOUDPICKLE = b"MWCP"  # MeshWeaver Cloudpickle
MAGIC_HEADER_PICKLE = b"MWPK"       # MeshWeaver Standard Pickle
MAGIC_HEADER_JSON = b"MWJS"         # MeshWeaver Pure JSON


class SerializationError(Exception):
    """Raised when serialization or deserialization encounters invalid or unsupported objects."""
    pass


class IntegrityError(SerializationError):
    """Raised when SHA-256 payload checksum validation fails during wire unpacking."""
    pass


class TaskSerializer:
    """
    High-fidelity serializer and payload packaging utility.
    Guarantees payload integrity and prevents execution of truncated/corrupted network buffers.
    """

    @classmethod
    def has_cloudpickle(cls) -> bool:
        """Indicates whether cloudpickle is available in the current environment."""
        return _HAS_CLOUDPICKLE

    @classmethod
    def serialize_callable(
        cls,
        func: Callable[..., Any],
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        secret_key: Optional[bytes] = None,
    ) -> bytes:
        """
        Serializes a Python function or closure along with positional and keyword arguments.
        Binary Format: [4 bytes MAGIC][32 bytes SHA-256][Serialized Byte Payload]
        """
        args = args or ()
        kwargs = kwargs or {}
        payload_tuple = (func, args, kwargs)

        if _HAS_CLOUDPICKLE:
            magic = MAGIC_HEADER_CLOUDPICKLE
            raw_data = cloudpickle.dumps(payload_tuple)
        else:
            magic = MAGIC_HEADER_PICKLE
            raw_data = pickle.dumps(payload_tuple, protocol=pickle.HIGHEST_PROTOCOL)

        # Compute SHA-256 or HMAC checksum for integrity
        if secret_key:
            checksum = hmac.new(secret_key, raw_data, hashlib.sha256).digest()
        else:
            checksum = hashlib.sha256(raw_data).digest()

        return magic + checksum + raw_data

    @classmethod
    def deserialize_callable(
        cls,
        packed_bytes: bytes,
        secret_key: Optional[bytes] = None,
    ) -> Tuple[Callable[..., Any], Tuple[Any, ...], Dict[str, Any]]:
        """
        Validates SHA-256 checksum and unpacks callable and execution arguments.
        """
        if len(packed_bytes) < 36:
            raise SerializationError("Payload too short to contain header and checksum")

        magic = packed_bytes[:4]
        expected_checksum = packed_bytes[4:36]
        raw_data = packed_bytes[36:]

        # Verify integrity
        if secret_key:
            computed_checksum = hmac.new(secret_key, raw_data, hashlib.sha256).digest()
        else:
            computed_checksum = hashlib.sha256(raw_data).digest()

        if not hmac.compare_digest(computed_checksum, expected_checksum):
            raise IntegrityError("Task payload integrity check failed (corrupted or tampered wire bytes)")

        try:
            if magic == MAGIC_HEADER_CLOUDPICKLE:
                if not _HAS_CLOUDPICKLE:
                    unpacked = pickle.loads(raw_data)
                else:
                    unpacked = cloudpickle.loads(raw_data)
            elif magic == MAGIC_HEADER_PICKLE:
                unpacked = pickle.loads(raw_data)
            else:
                raise SerializationError(f"Unknown serialization magic header: {magic}")
            
            func, args, kwargs = unpacked
            return func, args, kwargs
        except Exception as e:
            raise SerializationError(f"Failed to deserialize function payload: {e}") from e

    @classmethod
    def serialize_value(cls, val: Any) -> bytes:
        """Serializes task return values."""
        if _HAS_CLOUDPICKLE:
            return MAGIC_HEADER_CLOUDPICKLE + cloudpickle.dumps(val)
        return MAGIC_HEADER_PICKLE + pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize_value(cls, payload: bytes) -> Any:
        """Deserializes task return values."""
        if not payload:
            return None
        magic = payload[:4]
        data = payload[4:]
        if magic == MAGIC_HEADER_CLOUDPICKLE and _HAS_CLOUDPICKLE:
            return cloudpickle.loads(data)
        return pickle.loads(data)

    @classmethod
    def package_task(
        cls,
        name: str,
        func: Callable[..., Any],
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        origin_node_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Task:
        """Packages a Python callable into a ready-to-route Task instance."""
        payload = cls.serialize_callable(func, args, kwargs)
        return Task(
            name=name,
            payload=payload,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            origin_node_id=origin_node_id,
            metadata=metadata or {},
        )

    # ------------------------------------------------------------------
    # Standard Computational Benchmarks (Demo Workloads)
    # ------------------------------------------------------------------

    @staticmethod
    def workload_prime_sieve(limit: int) -> int:
        """CPU Benchmark: Sieve of Eratosthenes prime counter."""
        if limit < 2:
            return 0
        sieve = [True] * (limit + 1)
        sieve[0] = sieve[1] = False
        for i in range(2, int(math.sqrt(limit)) + 1):
            if sieve[i]:
                for j in range(i * i, limit + 1, i):
                    sieve[j] = False
        return sum(sieve)

    @staticmethod
    def workload_monte_carlo_pi(samples: int) -> float:
        """Math Benchmark: Monte Carlo Pi estimation."""
        inside = 0
        for _ in range(samples):
            x = random.random()
            y = random.random()
            if x * x + y * y <= 1.0:
                inside += 1
        return (4.0 * inside) / samples

    @staticmethod
    def workload_matrix_multiplication(dimension: int) -> float:
        """Compute Benchmark: Square matrix multiplication."""
        A = [[(i + j) % 10 for j in range(dimension)] for i in range(dimension)]
        B = [[(i * j) % 10 for j in range(dimension)] for i in range(dimension)]
        C = [[0] * dimension for _ in range(dimension)]
        for i in range(dimension):
            for k in range(dimension):
                for j in range(dimension):
                    C[i][j] += A[i][k] * B[k][j]
        return sum(sum(row) for row in C)

    @staticmethod
    def workload_synthetic_delay(seconds: float) -> str:
        """I/O Benchmark: Simulates network or disk latency."""
        time.sleep(seconds)
        return f"Completed delay of {seconds:.2f}s"

    @classmethod
    def create_benchmark_task(
        cls,
        benchmark_name: str,
        *args: Any,
        origin_node_id: Optional[str] = None,
        timeout_seconds: float = 30.0,
        **kwargs: Any,
    ) -> Task:
        """
        Factory helper to generate standardized workload tasks for demonstration and profiling.
        Supported benchmarks: 'primes', 'pi', 'matrix', 'delay'.
        """
        registry = {
            "primes": (cls.workload_prime_sieve, (100_000,) if not args else args),
            "pi": (cls.workload_monte_carlo_pi, (500_000,) if not args else args),
            "matrix": (cls.workload_matrix_multiplication, (120,) if not args else args),
            "delay": (cls.workload_synthetic_delay, (1.0,) if not args else args),
        }

        if benchmark_name not in registry:
            raise ValueError(f"Unknown benchmark: {benchmark_name}. Valid options: {list(registry.keys())}")

        fn, default_args = registry[benchmark_name]
        call_args = args if args else default_args
        return cls.package_task(
            name=f"benchmark_{benchmark_name}",
            func=fn,
            args=call_args,
            kwargs=kwargs,
            timeout_seconds=timeout_seconds,
            origin_node_id=origin_node_id,
            metadata={"benchmark_name": benchmark_name},
        )


def serialize_payload(obj: Any) -> bytes:
    """Convenience helper for serialize_value."""
    return TaskSerializer.serialize_value(obj)


def deserialize_payload(raw: bytes) -> Any:
    """Convenience helper for deserialize_value."""
    return TaskSerializer.deserialize_value(raw)
