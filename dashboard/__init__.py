"""
MeshWeaver Real-Time Terminal Dashboard Subsystem
Visualizes cluster topology, peer CPU/RAM utilization, gossip rounds,
and failure detection telemetry using Rich terminal components.
"""

from .cli import DashboardEventLogger, RichTerminalDashboard

__all__ = [
    "DashboardEventLogger",
    "RichTerminalDashboard",
]
