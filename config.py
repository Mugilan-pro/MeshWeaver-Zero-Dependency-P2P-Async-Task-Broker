"""
MeshWeaver Global Configuration
Default parameters for networking, task execution, routing, and fault tolerance.
"""

# Networking Defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
SOCKET_BUFFER_SIZE = 65536
SOCKET_TIMEOUT_SECONDS = 10.0

# Task Execution & Scheduling Defaults
DEFAULT_TASK_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_LEASE_TIMEOUT_SECONDS = 35.0
DEFAULT_LOCAL_CPU_THRESHOLD = 50.0  # Max CPU percentage to accept local execution

# Gossip & Heartbeat Defaults
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_TIMEOUT_SECONDS = 15.0  # Nodes missing heartbeats beyond this are flagged DEAD
GOSSIP_FANOUT = 3                  # Number of random peers to gossip to per round

# Dashboard Defaults
DEFAULT_DASHBOARD_REFRESH_SECONDS = 1.0
DEFAULT_DASHBOARD_MAX_EVENTS = 10
