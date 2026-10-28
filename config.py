"""
MeshWeaver Global Configuration
Default parameters for networking, task execution, routing, and fault tolerance.
"""

# Networking Defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
SOCKET_BUFFER_SIZE = 65536
SOCKET_TIMEOUT_SECONDS = 10.0

# Task Execution & Scheduling Defaults (Week 3)
DEFAULT_TASK_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_LEASE_TIMEOUT_SECONDS = 35.0
DEFAULT_LOCAL_CPU_THRESHOLD = 50.0  # Max CPU percentage to accept local execution
DEFAULT_ROUTING_STRATEGY = "LOWEST_CPU"  # Week 3 lowest CPU load routing

# Gossip & Heartbeat Defaults (Week 1 & 2 & 3)
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_TIMEOUT_SECONDS = 15.0  # Nodes missing heartbeats beyond this are flagged DEAD
GOSSIP_FANOUT = 3                  # Number of random peers to gossip to per round

# Security & Cryptography Defaults (Week 4)
ENABLE_TLS = True                  # Week 4 TLS/SSL encrypted communication
TLS_CERT_DIR = "certs"
DEFAULT_CLUSTER_SECRET = b"meshweaver-edge-secret-token-key-2026"  # Shared HMAC-SHA256 key
SIGNATURE_MAX_AGE_SECONDS = 60.0   # Maximum age in seconds for task signatures (anti-replay)
REQUIRE_CRYPTOGRAPHIC_SIGNATURES = True

# Dashboard Defaults (Week 4 Topology & Task State)
DEFAULT_DASHBOARD_REFRESH_SECONDS = 1.0
DEFAULT_DASHBOARD_MAX_EVENTS = 15
TOPOLOGY_REFRESH_SECONDS = 1.0
