"""
MeshWeaver Global Configuration
Default parameters for networking, DHT peer discovery, task execution, routing,
gossip telemetry, transport security, and terminal monitoring.
"""

# Core Networking Defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
SOCKET_BUFFER_SIZE = 65536
SOCKET_TIMEOUT_SECONDS = 10.0

# DHT & Peer Discovery Defaults (Kademlia)
DHT_DEFAULT_BOOTSTRAP_PORT = 8001
DHT_K_BUCKET_SIZE = 20             # Maximum contacts per k-bucket (Kademlia k)
DHT_ALPHA = 3                      # Concurrency factor for iterative lookups (Kademlia alpha)
DHT_ID_BITS = 160                  # Bit length of Node IDs (standard 160-bit keyspace)
DHT_RPC_TIMEOUT_SECONDS = 3.0      # RPC request timeout bound
DHT_REFRESH_INTERVAL_SECONDS = 300.0  # Bucket refresh interval (5 minutes)

# Task Execution & Scheduling Defaults
DEFAULT_TASK_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_LEASE_TIMEOUT_SECONDS = 35.0
DEFAULT_LOCAL_CPU_THRESHOLD = 50.0  # Max local CPU percentage to accept local execution

# Gossip Telemetry & Heartbeat Defaults
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_TIMEOUT_SECONDS = 15.0   # Nodes missing heartbeats beyond this are flagged DEAD
GOSSIP_FANOUT = 3                  # Number of random peers to gossip to per round

# Security & Transport Layer Encryption Defaults
TLS_ENABLED = True                 # Wrap TCP connections in TLS/SSL
TLS_CERT_DAYS = 365                # Certificate validity
REQUIRE_SIGNATURES = True          # Cryptographic signature required on all task requests
SIGNATURE_ALGORITHM = "Ed25519"    # High-speed elliptic-curve signatures (fallback to HMAC)
MAX_TIMESTAMP_DRIFT_SECONDS = 60.0 # Replay attack mitigation bound

# Real-Time Dashboard Defaults
DASHBOARD_REFRESH_SECONDS = 1.0    # Live CLI dashboard refresh rate
MAX_EVENT_LOG_ENTRIES = 50         # Circular buffer for UI event stream
