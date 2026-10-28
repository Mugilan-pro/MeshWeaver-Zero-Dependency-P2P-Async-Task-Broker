"""
MeshWeaver Storage Subsystem
Provides zero-dependency persistent storage for tasks, cluster metrics, and logs.
"""

from .db import MeshWeaverStorage

__all__ = [
    "MeshWeaverStorage",
]
