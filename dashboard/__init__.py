"""
MeshWeaver Real-Time Terminal Dashboard Subsystem
Visualizes cluster topology, peer CPU/RAM utilization, gossip rounds,
and failure detection telemetry using Rich terminal components.
"""

from .cli import DashboardEventLogger, RichTerminalDashboard
from .web_server import MeshWeaverWebServer

__all__ = [
    "DashboardEventLogger",
    "RichTerminalDashboard",
    "MeshWeaverWebServer",
]
