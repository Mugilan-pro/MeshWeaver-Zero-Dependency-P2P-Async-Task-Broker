# MeshWeaver: P2P Async Task Broker

MeshWeaver is a lightweight, decentralized peer-to-peer task broker written in Python. It avoids the need for central infrastructure (like Redis or RabbitMQ) by letting nodes form an asyncio-based mesh network where tasks are cryptographically authenticated, routed based on lowest CPU load, and automatically re-routed if a worker crashes.

---

## What is in this Repository

This repository contains the complete implementation for **Weeks 1, 2, 3, and 4** of the MeshWeaver Decentralized Task Broker:

### Week 1 — Mesh Setup, Pings & Task Serialization
- **`task/models.py`**: Distributed unit of work model with lifecycle tracking (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `TIMEOUT`).
- **`task/serializer.py`**: Binary function packaging with `cloudpickle` (falling back to standard library `pickle`), SHA-256 integrity checksums, and pre-packaged benchmarks.
- **`gossip/models.py`**: Wire-level heartbeat ping envelopes (`HEARTBEAT`, `METRICS_UPDATE`).
- **`gossip/metrics.py`**: Zero-dependency hardware telemetry collector (`psutil` fallback to POSIX `/proc` and `os.getloadavg`).

### Week 2 — Periodic Gossip Telemetry & Failure Detection
- **`gossip/monitor.py`**: Epidemic gossip engine broadcasting CPU/RAM loads to $k=3$ neighbors every 5.0 seconds.
- **`gossip/models.py`**: Anti-entropy digest vector reconciliation (`GossipDigest`, `GOSSIP_DELTA`).
- **`gossip/monitor.py`**: Two-tier heartbeat failure detector (`HEALTHY` $\to$ `SUSPECT` $\to$ `DEAD`).

### Week 3 — Task Routing & Fault Tolerance
- **`scheduler/router.py`**: Load-aware routing algorithm selecting the specific node with the lowest reported CPU load across the mesh (`LOWEST_CPU`, composite scoring, exclusion set during retries).
- **`scheduler/reliability.py`**: Active task lease management, automated orphaned task capture, and dynamic re-assignment upon worker dropout.
- **`scheduler/reliability.py`**: Bounded retry enforcement and Dead-Letter Queue (DLQ) isolation for permanently failing tasks.
- **Fault Recovery Bridge**: Connects `GossipMonitor` dropout events directly to `ReliabilityManager.handle_worker_dropout` for zero-data-loss workload recovery.

### Week 4 — TLS/SSL Security, Cryptographic Signatures & Live Dashboard
- **`security/tls.py`**: Transport Layer Security (TLS/SSL) wrapping with automatic X.509 self-signed certificate generation (RSA 2048-bit) and modern cipher suite negotiation.
- **`security/crypto.py`**: Cryptographic message authentication using HMAC-SHA256. Signs every task request and validates payload integrity before execution, blocking forged or tampered workloads and preventing replay attacks.
- **`dashboard/cli.py`**: Real-time CLI Terminal Dashboard featuring:
  - **Real-Time Mesh Topology**: Inter-node link graph, endpoints, TLS security badges (`🔒 TLS`), and RTT link latencies.
  - **Live Task Execution State**: In-flight leased tasks, assigned workers, retry counters, and Dead-Letter Queue (DLQ) inspection.
  - **Telemetry Meters**: Visual progress bars (`[████░░░░░░]`) for CPU% and RAM% across all nodes.
  - **Cluster Event Log**: Timestamped alerts for node join, routing decisions, heartbeats, and security rejections.
- **`network/`**: Framed binary network protocol and asynchronous TCP/TLS client/server transports with in-memory virtual routing for sandboxed environments.
- **`mesh/node.py`**: Unified `MeshNode` orchestrating execution, routing, reliability, gossip, TLS networking, and cryptographic security.
- **`cli.py` & `setup.py`**: Production-ready command-line interface and packaging for edge computing environments.

---

## Directory Structure
# MeshWeaver: Zero-Dependency P2P Async Task Broker

MeshWeaver is a decentralized, peer-to-peer task broker built entirely in pure Python. It eliminates centralized message queues (like Celery, Redis, or RabbitMQ) by forming a self-organizing mesh network where compute tasks are negotiated, load-balanced, and executed with automatic fault tolerance.

---

## Task Engine & Scheduling Architecture

This repository contains the core compute engine and decentralized scheduling subsystems:

- **`task.models`**: Declarative state machines (`PENDING`, `SCHEDULED`, `RUNNING`, `COMPLETED`, `FAILED`, `TIMEOUT`) and wire-compatible task/result models.
- **`task.serializer`**: High-fidelity callable serialization supporting dynamic closures via `cloudpickle` with fallback to standard library `pickle`/`json`, protected by SHA-256 integrity checksums.
- **`task.executor`**: Asynchronous worker execution engine offloading compute to worker pools to prevent event loop starvation, with strict execution timeout enforcement.
- **`scheduler.router`**: Decentralized workload distribution supporting Least-CPU Load and Composite Load balancing.
- **`scheduler.reliability`**: Lease tracking, node crash detection, bounded retries, and automatic task reassignment.

---

## Directory Structure

```
MeshWeaver/
├── config.py                           # Global default configuration settings
├── task/                               # Core task abstraction and execution engine
│   ├── __init__.py
│   ├── models.py                       # Task, TaskResult, TaskStatus, TaskPriority
│   ├── serializer.py                   # Checksummed serializer & math benchmarks
│   └── executor.py                     # Asynchronous pool executor with timeouts
├── scheduler/                          # Workload distribution and fault tolerance
│   ├── __init__.py
│   ├── router.py                       # Load-aware worker selection algorithms
│   └── reliability.py                  # Lease manager & dropout recovery
├── tests/                              # Automated test suites
│   ├── test_tasks.py                   # Task models, serializer, and executor tests
│   └── test_scheduler.py               # Router and reliability recovery tests
├── examples/                           # Interactive demonstration scripts
│   ├── task_execution_demo.py          # Serialization and compute benchmark demo
│   └── task_distribution_demo.py      # Network routing & crash recovery demo
└── docs/
    └── task_engine_architecture.md     # Subsystem specification & API contracts
# MeshWeaver: Decentralized P2P Task Broker

MeshWeaver is a **pure-Python, zero-dependency** peer-to-peer task broker. It replaces centralized task brokers (like Celery, Redis, or RabbitMQ) by forming a decentralized mesh network where compute tasks are negotiated, load-balanced, and executed across peers — using only Python's standard library.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Module Breakdown](#module-breakdown)
  - [DHT & Peer Discovery](#dht--peer-discovery)
  - [Gossip Protocol & Failure Detection](#gossip-protocol--failure-detection)
  - [Task Engine & Serialization](#task-engine--serialization)
  - [Scheduler & Fault Tolerance](#scheduler--fault-tolerance)
  - [Persistent Storage](#persistent-storage)
  - [Network Layer](#network-layer)
  - [Web Dashboard](#web-dashboard)
  - [Terminal CLI Dashboard](#terminal-cli-dashboard)
- [Running the Code](#running-the-code)
- [Tests](#tests)
- [Architecture Docs](#architecture-docs)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        MeshWeaver Cluster                            │
│                                                                      │
│  ┌─────────────┐    Kademlia DHT     ┌─────────────┐                │
│  │  Node A     │◄───────────────────►│  Node B     │                │
│  │             │    Gossip / UDP     │             │                │
│  │  TaskRouter │◄───────────────────►│  TaskRouter │                │
│  │  Executor   │    TCP Tasks        │  Executor   │                │
│  │  SQLite DB  │                     │  SQLite DB  │                │
│  └──────┬──────┘                     └──────┬──────┘                │
│         │                                   │                        │
│         └──────────── Web Dashboard ────────┘                        │
│                     http://127.0.0.1:8080                            │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
MeshWeaver/
├── config.py                           # Global network, DHT, gossip & task settings
│
├── dht/                                # Kademlia DHT & peer discovery
│   ├── node_id.py                      # 160-bit NodeID and XOR distance metric
│   ├── routing.py                      # K-bucket routing table (k=20, LRU cache)
│   └── discovery.py                    # Asyncio UDP RPCs (PING, FIND_NODE), bootstrap & alpha-lookup
│
├── gossip/                             # Gossip protocol & failure detection
│   ├── models.py                       # PeerNodeRecord, NodeMetrics, GossipMessage models
│   ├── metrics.py                      # CPU/RAM collector (psutil or /proc fallback)
│   └── monitor.py                      # GossipMonitor: heartbeats, SUSPECT/DEAD detection, callbacks
│
├── task/                               # Task engine & serialization
│   ├── models.py                       # Task, TaskResult, TaskStatus, TaskPriority models
│   ├── serializer.py                   # Pickle + SHA-256 integrity serializer
│   └── executor.py                     # Async worker pool executor
│
├── scheduler/                          # Workload routing & fault tolerance
│   ├── router.py                       # Load-aware worker selection (LOWEST_CPU, ROUND_ROBIN, RANDOM)
│   └── reliability.py                  # Lease tracking & dropout reassignment (dead-letter queue)
│
├── storage/                            # Zero-dependency persistent storage
│   └── db.py                           # SQLite WAL engine: tasks, audit events, node snapshots
│
├── network/                            # TCP transport layer
│   ├── protocol.py                     # Message framing & protocol constants
│   ├── server.py                       # Async TCP server (asyncio)
│   └── client.py                       # Async TCP client with reconnect
│
├── dashboard/                          # Monitoring & control interfaces
│   ├── web_server.py                   # Zero-dependency HTTP server + REST API
│   ├── cli.py                          # Terminal TUI (rich library or ANSI fallback)
│   └── static/
│       └── index.html                  # Single-page Web Dashboard (canvas topology, task dispatch)
│
├── examples/                           # Runnable demos
│   ├── web_dashboard_demo.py           # Full cluster demo + Web Dashboard (http://127.0.0.1:8080)
│   ├── gossip_dashboard_demo.py        # Terminal live gossip monitor demo
│   ├── gossip_telemetry_demo.py        # Gossip telemetry & metric collection demo
│   ├── task_execution_demo.py          # Compute benchmark demo (primes, matrix, Pi)
│   ├── task_distribution_demo.py       # Load routing & worker crash recovery demo
│   ├── week1_2_gossip_demo.py          # Gossip weeks 1 & 2 walkthrough
│   ├── member4_demo.py                 # Task engine quick demo
│   └── member5_demo.py                 # Gossip member demo
│
├── dht_discovery_demo.py               # DHT discovery & routing standalone demo
├── network_audit_demo.py               # 10-node dynamic mesh audit (network-level)
│
├── tests/                              # Automated test suite (11 test files)
│   ├── test_dht.py                     # DHT unit tests (NodeID, routing, discovery)
│   ├── test_gossip.py                  # Gossip protocol tests
│   ├── test_health.py                  # Peer health state & failure detection tests
│   ├── test_tasks.py                   # Task serialization & execution tests
│   ├── test_scheduler.py               # Workload routing & recovery tests
│   ├── test_storage.py                 # SQLite persistence tests
│   ├── test_network.py                 # TCP transport layer tests
│   ├── test_web_dashboard.py           # Web server API tests
│   ├── test_member5_gossip.py          # Member 5 gossip integration tests
│   └── test_member4_tasks.py           # Member 4 task integration tests
│
└── docs/                               # Architecture specifications
    ├── dht_architecture.md             # DHT specification & RPC protocol
    ├── task_engine_architecture.md     # Task engine architecture
    ├── gossip_fault_tolerance_architecture.md  # Gossip & failure detection design
    └── member5_interface.md            # Member 5 interface contracts
```

---


## Execution Guide

### 1. Run Automated Unit Tests

Test task packaging, serialization integrity, and asynchronous execution:
```bash
python -m unittest tests/test_tasks.py
```


Test load-based routing and crash recovery:
```bash
python -m unittest tests/test_scheduler.py
```
MeshWeaver/
├── cli.py                          # Unified CLI entrypoint (nodes, submit, dashboard, demos)
├── setup.py                        # Python packaging configuration for edge deployment
├── config.py                       # Global settings (ports, timeouts, TLS, HMAC secrets)
├── mesh/                           # Unified P2P Mesh Node Orchestrator
│   ├── __init__.py
│   └── node.py                     # MeshNode implementation
├── security/                       # Week 4 Security & Cryptography Layer
│   ├── __init__.py
│   ├── tls.py                      # TLS/SSL context management & X.509 cert generation
│   └── crypto.py                   # HMAC-SHA256 task request signing & replay prevention
├── network/                        # Asynchronous Networking Transport & Wire Framing
│   ├── __init__.py
│   ├── protocol.py                 # Binary framed NetworkPacket definitions
│   └── transport.py                # Asyncio TCP/TLS MeshServer, MeshClient & In-Memory Router
├── scheduler/                      # Week 3 Task Router & Reliability Subsystem
│   ├── __init__.py
│   ├── router.py                   # Lowest-CPU workload selection & composite load scoring
│   └── reliability.py              # Lease tracking, dropout re-routing & Dead-Letter Queue
├── task/                           # Task Engine & Serialization Subsystem
│   ├── __init__.py
│   ├── models.py                   # Task and TaskResult lifecycle models
│   ├── serializer.py               # Cloudpickle/pickle serialization & SHA-256 integrity
│   └── executor.py                 # Async isolated worker pools with execution timeouts
├── gossip/                         # Gossip Protocol, Telemetry & Failure Detection
│   ├── __init__.py
│   ├── metrics.py                  # Zero-dependency CPU/RAM hardware sampling
│   ├── models.py                   # PeerNodeRecord, GossipMessage, NodeMetrics, Digest
│   └── monitor.py                  # 5s gossip loop & two-tier heartbeat failure detector
├── dashboard/                      # Real-Time Terminal Dashboard
│   ├── __init__.py
│   └── cli.py                      # Mesh topology visualizer & task execution state table
├── tests/                          # Comprehensive Automated Test Suites (91 Unit Tests)
│   ├── test_week3_routing_fault_tolerance.py  # Week 3 dedicated test suite
│   ├── test_week4_security_dashboard.py       # Week 4 dedicated test suite
│   ├── test_mesh_node_integration.py          # End-to-end multi-node mesh integration tests
│   ├── test_member5_gossip.py                 # Member 5 all-weeks test runner
│   ├── test_member4_tasks.py                  # Member 4 test runner
│   ├── test_tasks.py                          # Serialization & execution unit tests
│   ├── test_scheduler.py                      # Scheduler routing & reliability tests
│   ├── test_gossip.py                         # Metrics sampling & gossip protocol tests
│   └── test_health.py                         # Heartbeat state machine & dropout tests
├── examples/                       # Executable Demonstrations
│   ├── week3_routing_demo.py                  # Week 3 Lowest-CPU Routing & Crash Recovery Demo
│   ├── week4_security_demo.py                 # Week 4 TLS, HMAC-SHA256 & Topology Dashboard Demo
│   ├── week3_4_full_mesh_demo.py              # Complete Weeks 1-4 End-to-End Cluster Demo
│   ├── member5_week3_4_demo.py                # Member 5 Weeks 3 & 4 Milestone Demo
│   ├── week1_2_gossip_demo.py                 # Weeks 1 & 2 Milestone Demo
│   ├── gossip_dashboard_demo.py               # Live Terminal Dashboard Demo
│   ├── member5_demo.py                        # Member 5 Demo Launcher
│   ├── member4_demo.py                        # Member 4 Demo Launcher
│   ├── task_distribution_demo.py              # Workload Distribution Demo
│   └── task_execution_demo.py                 # Workload Packaging Demo
└── docs/                           # Architectural Documentation & Specifications
    ├── week3_week4_architecture.md            # Weeks 3 & 4 Comprehensive Architecture
    ├── gossip_fault_tolerance_architecture.md # Gossip & Failure Detection Architecture
    ├── member5_interface.md                   # Member 5 Interface Specification
    └── task_engine_architecture.md            # Task Engine Architecture Specification

### 2. Run Interactive Demos

Run the Task Execution & Serialization Benchmark:
```bash
python examples/task_execution_demo.py
```

Run the Decentralized Distribution & Node Failure Recovery Simulation:
```bash
python examples/task_distribution_demo.py
```
=======
## Module Breakdown

### DHT & Peer Discovery

**Location:** `dht/`

Implements the [Kademlia](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) distributed hash table for decentralized peer discovery — no hardcoded IP lists required.

| File | Responsibility |
|------|----------------|
| `node_id.py` | 160-bit keyspace with SHA-1 hashing, XOR distance metric $d(x,y)=x \oplus y$, bitwise prefix matching |
| `routing.py` | 160 k-buckets ($k=20$) with LRU ordering, replacement cache for full buckets, $O(\log N)$ closest-peer search |
| `discovery.py` | Asyncio UDP RPC protocol (`PING`, `PONG`, `FIND_NODE`, `FOUND_NODES`), dynamic bootstrap join, $\alpha=3$ concurrent iterative lookup |

---

### Gossip Protocol & Failure Detection

**Location:** `gossip/`

Implements a push-based gossip heartbeat protocol. Each node periodically broadcasts its metrics to a random subset of peers (fanout=3). Missed heartbeats escalate through `HEALTHY → SUSPECT → DEAD`.

| File | Responsibility |
|------|----------------|
| `models.py` | `PeerNodeRecord`, `NodeMetrics`, `GossipMessage`, `PeerHealthState` (HEALTHY/SUSPECT/DEAD) |
| `metrics.py` | CPU/RAM/uptime collector — uses `psutil` if installed, falls back to `/proc/stat`, `getloadavg`, or `process_time` |
| `monitor.py` | `GossipMonitor`: background heartbeat loop, peer timeout tracking, pluggable callbacks (`on_peer_dropout`, `on_peer_recovered`, `on_peer_suspect`) |

**Key config (`config.py`):**
```python
HEARTBEAT_INTERVAL_SECONDS = 5.0   # Gossip broadcast interval
HEARTBEAT_TIMEOUT_SECONDS  = 15.0  # Node marked DEAD after this
GOSSIP_FANOUT              = 3     # Peers per gossip round
```

---

### Task Engine & Serialization

**Location:** `task/`

Handles task packaging, integrity verification, async execution, and result reporting.

| File | Responsibility |
|------|----------------|
| `models.py` | `Task` (id, func, args, priority, timeout), `TaskResult` (status, value, exec time), `TaskPriority` (LOW/NORMAL/HIGH/CRITICAL), `TaskStatus` |
| `serializer.py` | Pickle serializer with SHA-256 digest for payload integrity — detects tampering or corruption |
| `executor.py` | Async worker pool — executes callables with timeout enforcement, captures exceptions as `TaskResult.failure()` |

**Built-in compute benchmarks (used in Web Dashboard):**
- `PrimeCount` — Sieve of Eratosthenes
- `MonteCarloPi` — Monte Carlo π estimation
- `MatrixMultiply` — Pure-Python matrix multiplication
- `SHA256Chain` — Iterated SHA-256 hash chain
- `Fibonacci` — Iterative Fibonacci

---

### Scheduler & Fault Tolerance

**Location:** `scheduler/`

Routes tasks to the best available worker and handles worker crashes gracefully.

| File | Responsibility |
|------|----------------|
| `router.py` | `TaskRouter` — maintains `WorkerNodeInfo` registry, selects workers via `LOWEST_CPU`, `ROUND_ROBIN`, or `RANDOM` strategy |
| `reliability.py` | `ReliabilityManager` — lease-based task tracking, reassigns orphaned tasks on worker dropout, dead-letter queue for unrecoverable failures |

---

## How to Run
### Persistent Storage

**Location:** `storage/db.py`

Zero-dependency SQLite storage using Python's built-in `sqlite3` with **Write-Ahead Logging (WAL)** for concurrent read/write safety.

**Three tables:**

| Table | Content |
|-------|---------|
| `tasks` | Task records (id, name, status, priority, worker, exec time, result, error) |
| `audit_events` | Timestamped cluster event log (GOSSIP, INFO, WARN, ALERT, TASK) |
| `node_snapshots` | Time-series telemetry snapshots (cpu, ram, active_tasks, status per node) |

**Thread-safe** via per-thread SQLite connections (`threading.local`). Singleton accessor: `MeshWeaverStorage.get_instance()`.

---

### Network Layer

**Location:** `network/`

Pure asyncio TCP transport for inter-node communication.

| File | Responsibility |
|------|----------------|
| `protocol.py` | Length-prefixed message framing, message type constants, encode/decode |
| `server.py` | Async TCP server — accepts connections, dispatches to registered handlers |
| `client.py` | Async TCP client with auto-reconnect and configurable timeout |

### 1. Run Automated Unit Tests

Run all 91 unit tests across all project milestones:
```bash
python3 -m unittest discover tests
```
### Web Dashboard

**Location:** `dashboard/web_server.py` + `dashboard/static/index.html`

A fully self-contained browser dashboard served by a zero-dependency Python HTTP server (no Flask, no Django).

**REST API Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Serves the single-page dashboard UI |
| `GET` | `/api/status` | Cluster health summary (nodes, tasks, CPU/RAM, uptime) |
| `GET` | `/api/nodes` | Per-node telemetry (cpu, ram, status, heartbeat age) |
| `GET` | `/api/events` | Recent audit event log |
| `GET` | `/api/tasks` | Persisted task history from SQLite |
| `POST` | `/api/tasks` | Dispatch a compute task (preset, strategy, priority) |
| `POST` | `/api/simulate/crash` | Simulate a peer node going offline |
| `POST` | `/api/simulate/recover` | Restore a crashed peer to HEALTHY |
| `POST` | `/api/simulate/fluctuate` | Randomly shift peer CPU/RAM metrics |

**Dashboard Features:**
- 🕸️ Canvas mesh topology (animated node graph)
- 📊 Live cluster metrics strip (nodes, tasks, CPU, RAM, uptime)
- 🖥️ Per-node telemetry cards with health badges
- ⚡ Task dispatch panel (5 built-in compute benchmarks, 3 routing strategies)
- 📋 Persistent task history table (live-polled every 5s from SQLite)
- 🎭 Chaos simulation controls (crash, recover, fluctuate)
- 📝 Real-time event log feed

Run Week 3's dedicated test suite (Routing & Fault Tolerance):
```bash
python3 -m unittest tests/test_week3_routing_fault_tolerance.py
```

Run Week 4's dedicated test suite (TLS, Cryptographic Signatures & Topology Dashboard):
```bash
python3 -m unittest tests/test_week4_security_dashboard.py
```

Run the End-to-End Multi-Node Integration test suite:
```bash
python3 -m unittest tests/test_mesh_node_integration.py
```

Run Member 5's comprehensive test runner (Weeks 1, 2, 3, and 4):
```bash
python3 -m unittest tests/test_member5_gossip.py
```

---

### 2. Run the Demos

#### Complete Multi-Node Cluster Demo (Weeks 1, 2, 3 & 4)
Simulates a 4-node cluster with TLS and HMAC signing, exchanges telemetry, routes tasks to the lowest-CPU node, recovers from a simulated worker crash, and displays the dashboard:
```bash
python3 examples/week3_4_full_mesh_demo.py
```
### Terminal CLI Dashboard

**Location:** `dashboard/cli.py`

Full-featured terminal monitor. Uses the `rich` library when installed for a polished TUI; falls back to ANSI box-drawing characters when running in minimal environments.

```
╔══════════════════════════════════════════════════════════════╗
║       MeshWeaver P2P Task Broker — Cluster Monitor           ║
╠══════════╦═══════════╦════════════════╦════════════╦═════════╣
║ Node ID  ║  Status   ║ CPU Usage      ║ RAM Usage  ║ Hrtbt   ║
╠══════════╬═══════════╬════════════════╬════════════╬═════════╣
║ ★ local  ║ HEALTHY   ║ [████░░] 42.0% ║ [██░░] 28% ║ 0.0s    ║
║ worker_1 ║ HEALTHY   ║ [██░░░░] 24.5% ║ [████] 42% ║ 1.2s    ║
║ worker_2 ║ DEAD      ║ [░░░░░░] 0.0%  ║ [░░░░] 0%  ║ DEAD    ║
╚══════════╩═══════════╩════════════════╩════════════╩═════════╝
```

---

## Running the Code

### 1. Start the Web Dashboard (Recommended)

Spins up a 5-node simulated cluster and opens the browser dashboard:

#### Week 3 Milestone Demo (Lowest-CPU Routing & Fault Tolerance)
Demonstrates load-aware routing selecting the lowest CPU peer, worker dropout detection, automatic task reassignment, and Dead-Letter Queue (DLQ) bounded retry isolation:
```bash
python3 examples/week3_routing_demo.py
```

#### Week 4 Milestone Demo (TLS Transport, HMAC Signatures & Live Dashboard)
Demonstrates X.509 certificate generation, encrypted transport handshake, HMAC-SHA256 task signing, adversary tamper rejection, replay protection, and the live mesh topology dashboard:
```bash
python3 examples/week4_security_demo.py
```
python examples/web_dashboard_demo.py
# Dashboard available at http://127.0.0.1:8080
```

### 2. Run the DHT Network Audit (10 Nodes)

Proves 10 local DHT instances dynamically discover each other with zero hardcoded peer lists:

#### Member 5 Weeks 3 & 4 Demo
Demonstrates Member 5's failure detection bridge and real-time topology dashboard:
```bash
python3 examples/member5_week3_4_demo.py
```

---

### 3. Using the MeshWeaver CLI

MeshWeaver includes a zero-dependency CLI tool (`cli.py`):

```bash
# Display help and available commands
python3 cli.py --help

# Generate self-signed TLS certificates for a node
python3 cli.py certs --node-id node_alpha

# View the real-time cluster dashboard
python3 cli.py dashboard --node-id my_node --peer worker1:8001 --peer worker2:8002

# Run any project milestone demonstration
python3 cli.py demo week3
python3 cli.py demo week4
python3 cli.py demo full
```

---

## Deliverables Matrix (Weeks 1, 2, 3 & 4)

| Week | Project Requirement | Implementation | Automated Verification |
|---|---|---|---|
| **Week 1** | Peer heartbeat ping / socket communication | `gossip/models.py` (`HEARTBEAT` message type) | `test_heartbeat_ping_wire_serialization` |
| **Week 1** | Hardware telemetry metrics sampling (CPU/RAM) | `gossip/metrics.py` (`SystemMetricsCollector`) | `test_metrics_collection_baseline` |
| **Week 1** | Task serialization with SHA-256 verification | `task/serializer.py` (`TaskSerializer`) | `test_serialize_and_deserialize_callable` |
| **Week 2** | Periodic gossip loop sharing CPU/RAM every 5s | `gossip/monitor.py` (`_gossip_loop`, 5s interval) | `test_periodic_metrics_broadcast_and_receipt` |
| **Week 2** | Anti-entropy state digest synchronization | `gossip/models.py` (`GossipDigest`, delta sync) | `test_anti_entropy_digest_and_delta` |
| **Week 2** | Two-tier failure detector (`HEALTHY` $\to$ `SUSPECT` $\to$ `DEAD`) | `gossip/monitor.py` (`check_peer_health`) | `test_transition_to_dead` |
| **Week 3** | Workload routing to node with lowest reported CPU | `scheduler/router.py` (`RoutingStrategy.LOWEST_CPU`) | `test_routing_selects_lowest_cpu_node` |
| **Week 3** | Dynamic telemetry adaptation & failure exclusion | `scheduler/router.py` (`update_worker_metrics`, `exclude`) | `test_routing_dynamic_load_update` |
| **Week 3** | Continuous heartbeat failure monitor | `gossip/monitor.py` (`check_peer_health`) | `test_worker_dropout_detection` |
| **Week 3** | Automatic task recovery & re-routing on crash | `scheduler/reliability.py` (`handle_worker_dropout`) | `test_automatic_task_rerouting_on_worker_dropout` |
| **Week 3** | Bounded retry limits & Dead-Letter Queue (DLQ) | `scheduler/reliability.py` (`record_failure_and_retry`) | `test_retry_exhaustion_moves_task_to_dead_letter_queue` |
| **Week 4** | TLS/SSL node communication wrapping | `security/tls.py` (`create_server_ssl_context`, `MemoryBIO`) | `test_in_memory_tls_handshake_and_encryption` |
| **Week 4** | X.509 self-signed certificate generation | `security/tls.py` (`generate_self_signed_cert`) | `test_x509_certificate_generation` |
| **Week 4** | Cryptographic signatures for task execution requests | `security/crypto.py` (`sign_task_request`, HMAC-SHA256) | `test_sign_and_verify_valid_task` |
| **Week 4** | Tampering detection & forged task rejection | `security/crypto.py` (`verify_task_request`) | `test_tamper_detection_on_payload` |
| **Week 4** | Anti-replay attack timestamp enforcement | `security/crypto.py` (`verify_task_request`, 60s window) | `test_replay_attack_rejection` |
| **Week 4** | Real-time mesh topology view in CLI dashboard | `dashboard/cli.py` (`RichTerminalDashboard`) | `test_dashboard_renders_mesh_topology` |
| **Week 4** | Live task execution state table (leases, retries, DLQ) | `dashboard/cli.py` (`RichTerminalDashboard`) | `test_dashboard_renders_task_execution_state` |
| **Review**| Pure Python package ready for edge deployment | `setup.py`, `cli.py`, `mesh/node.py` | `test_end_to_end_lowest_cpu_routing_and_execution` |
python network_audit_demo.py
```

### 3. Run Interactive Demos

```bash
# Gossip protocol telemetry demo
python examples/gossip_telemetry_demo.py

# Terminal live monitor (rich TUI or ANSI fallback)
python examples/gossip_dashboard_demo.py

# Compute benchmark task execution
python examples/task_execution_demo.py

# Task distribution & crash recovery
python examples/task_distribution_demo.py

# DHT discovery & routing walkthrough
python dht_discovery_demo.py
```

### 4. Run Unit Tests

```bash
# All tests
python -m unittest discover tests/

# Individual test suites
python -m unittest tests/test_dht.py         # DHT (node IDs, routing, discovery)
python -m unittest tests/test_gossip.py      # Gossip protocol
python -m unittest tests/test_health.py      # Failure detection & health states
python -m unittest tests/test_tasks.py       # Task serialization & execution
python -m unittest tests/test_scheduler.py   # Workload routing & recovery
python -m unittest tests/test_storage.py     # SQLite persistence
python -m unittest tests/test_network.py     # TCP transport layer
python -m unittest tests/test_web_dashboard.py  # Web server API
```

---

## Tests

The test suite covers all major subsystems across **11 test files**:

| Test File | Subsystem | Coverage |
|-----------|-----------|----------|
| `test_dht.py` | DHT | NodeID XOR distance, k-bucket ops, iterative lookup |
| `test_gossip.py` | Gossip | Message propagation, peer record updates |
| `test_health.py` | Failure detection | HEALTHY→SUSPECT→DEAD transitions, callbacks |
| `test_tasks.py` | Task engine | Serialization, SHA-256 integrity, execution |
| `test_scheduler.py` | Scheduler | Routing strategies, dropout reassignment |
| `test_storage.py` | Storage | Task CRUD, event logging, node snapshots |
| `test_network.py` | Network | TCP framing, client/server handshake |
| `test_web_dashboard.py` | Web API | REST endpoints, task dispatch, simulations |
| `test_member5_gossip.py` | Gossip integration | End-to-end gossip cycle |
| `test_member4_tasks.py` | Task integration | Full task lifecycle |

---

## Architecture Docs

Detailed design specifications are in `docs/`:

| Document | Contents |
|----------|----------|
| [`dht_architecture.md`](docs/dht_architecture.md) | Kademlia keyspace, RPC protocol, bucket refresh |
| [`task_engine_architecture.md`](docs/task_engine_architecture.md) | Task lifecycle, serializer, executor pool design |
| [`gossip_fault_tolerance_architecture.md`](docs/gossip_fault_tolerance_architecture.md) | Gossip rounds, heartbeat escalation, failure callbacks |
| [`member5_interface.md`](docs/member5_interface.md) | Member 5 subsystem interface contracts |

---

## Dependencies

**Zero external dependencies required.** MeshWeaver runs entirely on the Python standard library.

Optional enhancements (installed separately):
- `psutil` — more accurate CPU/RAM metrics (falls back to `/proc` or `os` APIs)
- `rich` — polished terminal TUI dashboard (falls back to ANSI box rendering)

