# MeshWeaver: Decentralized P2P Task Broker

MeshWeaver is a pure Python peer-to-peer task broker. It replaces centralized task brokers (like Celery, Redis, or RabbitMQ) by forming a decentralized mesh network where compute tasks are negotiated, load-balanced, and executed across peers.

---

## Repository Structure

```
mesh-weaver/
├── config.py                       # Global network, DHT, and task settings
├── dht/                            # Member 3: Kademlia DHT & peer discovery
│   ├── __init__.py
│   ├── node_id.py                  # 160-bit NodeID and XOR distance metric
│   ├── routing.py                  # K-bucket routing table (k=20, LRU cache)
│   └── discovery.py                # Asyncio UDP RPCs (PING, FIND_NODE), bootstrap & alpha-lookup
├── task/                           # Member 4: Task engine & serialization
│   ├── __init__.py
│   ├── models.py                   # Task and TaskResult models
│   ├── serializer.py               # Serializer with SHA-256 integrity verification
│   └── executor.py                 # Async worker pool executor
├── scheduler/                      # Member 4: Workload routing & fault tolerance
│   ├── __init__.py
│   ├── router.py                   # Load-aware worker selection (least CPU)
│   └── reliability.py              # Lease tracking & dropout reassignment
├── tests/                          # Automated unit tests
│   ├── test_dht.py                 # Member 3 DHT unit tests (13 tests)
│   ├── test_tasks.py               # Task serialization & execution tests
│   └── test_scheduler.py           # Workload routing & recovery tests
├── examples/                       # Demos & network audits
│   ├── dht_discovery_demo.py       # DHT peer discovery walkthrough
│   ├── network_audit_demo.py       # Mid-Project Review: 10-node dynamic mesh audit
│   ├── task_execution_demo.py      # Math compute benchmark demo
│   └── task_distribution_demo.py  # Load routing & worker crash recovery demo
└── docs/                           # Architecture specifications
    ├── dht_architecture.md         # Member 3 DHT specification & RPC protocol
    └── task_engine_architecture.md # Member 4 Task engine architecture
```

---

## Member 3 Deliverables (Weeks 1 & 2)

As **Member 3 (DHT & Peer Discovery Developer)**, the following deliverables are completed:

1. **Node IDs & XOR Distance (`dht/node_id.py`)**:
   - 160-bit keyspace with SHA-1 hashing.
   - XOR distance metric $d(x, y) = x \oplus y$.
   - Bitwise prefix matching and $O(1)$ k-bucket index calculation.
2. **K-Bucket Routing Table (`dht/routing.py`)**:
   - 160 k-buckets ($k=20$) with LRU ordering.
   - Replacement cache to handle full buckets and promote active nodes on eviction.
   - $O(\log N)$ closest-peer search sorted strictly by XOR distance.
3. **Asyncio UDP Peer Discovery & Bootstrap (`dht/discovery.py`)**:
   - Lightweight RPC protocol (`PING`, `PONG`, `FIND_NODE`, `FOUND_NODES`).
   - Dynamic bootstrap join (no hardcoded IP lists needed).
   - $\alpha$-concurrent iterative node lookup ($\alpha=3$).
4. **Mid-Project Review Network Audit (`examples/network_audit_demo.py`)**:
   - Proves 10 local instances dynamically join via a single bootstrap seed and discover each other to form a complete mesh.

---

## Running the Code

### 1. Run Unit Tests

```bash
# Member 3 DHT unit tests (13 tests)
python -m unittest tests/test_dht.py

# Member 4 tests
python -m unittest tests/test_tasks.py
python -m unittest tests/test_scheduler.py
```

### 2. Run Mid-Project Review Network Audit (10 Nodes)

```bash
python examples/network_audit_demo.py
```

### 3. Run Interactive Demos

```bash
# Web Dashboard & Real-Time Topology Monitor (Runs on http://127.0.0.1:8080)
python examples/web_dashboard_demo.py

# DHT discovery & routing demo
python examples/dht_discovery_demo.py

# Task engine compute benchmarks
python examples/task_execution_demo.py

# Task distribution & crash recovery demo
python examples/task_distribution_demo.py

# Terminal live monitor (Rich TUI fallback)
python examples/gossip_dashboard_demo.py
```
