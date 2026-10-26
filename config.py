"""
MeshWeaver Global Configuration
Default parameters for networking, DHT peer discovery, task execution, routing, and fault tolerance.
"""

# Networking Defaults (Member 2)
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
SOCKET_BUFFER_SIZE = 65536
SOCKET_TIMEOUT_SECONDS = 10.0

# DHT & Peer Discovery Defaults (Member 3)
DHT_DEFAULT_BOOTSTRAP_PORT = 8001
DHT_K_BUCKET_SIZE = 20             # Maximum contacts per k-bucket (Kademlia k)
DHT_ALPHA = 3                      # Concurrency factor for iterative lookups (Kademlia alpha)
DHT_ID_BITS = 160                  # Bit length of Node IDs (standard Kademlia 160-bit keyspace)
DHT_RPC_TIMEOUT_SECONDS = 3.0      # RPC request timeout bound
DHT_REFRESH_INTERVAL_SECONDS = 300.0  # Bucket refresh interval (5 minutes)

# Task Execution & Scheduling Defaults (Member 4)
DEFAULT_TASK_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_LEASE_TIMEOUT_SECONDS = 35.0
DEFAULT_LOCAL_CPU_THRESHOLD = 50.0  # Max CPU percentage to accept local execution

# Gossip & Heartbeat Defaults (Member 5)
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_TIMEOUT_SECONDS = 15.0  # Nodes missing heartbeats beyond this are flagged DEAD
GOSSIP_FANOUT = 3                  # Number of random peers to gossip to per round

# Dashboard Defaults
DEFAULT_DASHBOARD_REFRESH_SECONDS = 1.0
DEFAULT_DASHBOARD_MAX_EVENTS = 10
