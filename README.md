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
```

---

## How to Run

### 1. Run Automated Unit Tests

Run all 91 unit tests across all project milestones:
```bash
python3 -m unittest discover tests
```

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
