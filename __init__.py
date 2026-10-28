"""
MeshWeaver Real-Time Terminal Dashboard
Built with 'rich' to visualize mesh topology, DHT routing, live gossip metrics,
and task execution states.
"""

from .cli import MeshDashboard, render_cluster_snapshot

__all__ = ["MeshDashboard", "render_cluster_snapshot"]
