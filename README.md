# MeshWeaver: Decentralized P2P Async Task Broker

MeshWeaver is a **pure-Python, zero-dependency** peer-to-peer task broker for distributed systems and edge-computing environments. It completely eliminates centralized message brokers (such as Celery, Redis, or RabbitMQ) by forming a self-organizing mesh network where compute tasks are negotiated, cryptographically verified, load-balanced, and executed across peers — using only Python's standard library.

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
├── setup.py                            # Edge-deployment package specification
├── pyproject.toml                      # Modern Python packaging standard
├── meshweaver_cli.py                   # Unified CLI tool (status, benchmark, test, demo)
├── config.py                           # Global network, DHT, gossip & task settings
│
├── task/                               # Task engine, execution & security (Member 4)
│   ├── models.py                       # Task, TaskResult, TaskStatus, TaskPriority
│   ├── serializer.py                   # Checksummed serializer & math benchmarks
│   ├── security.py                     # HMAC-SHA256 task signing & verification
│   └── executor.py                     # Asynchronous pool executor with security gates
│
├── scheduler/                          # Workload routing & fault tolerance (Member 4)
│   ├── router.py                       # Load-aware worker selection (Lowest CPU, Composite)
│   └── reliability.py                  # Lease tracking, async watchdog & DLQ recovery
│
├── dht/                                # Kademlia DHT & peer discovery (Member 3)
│   ├── node_id.py                      # 160-bit NodeID and XOR distance metric
│   ├── routing.py                      # K-bucket routing table (k=20, LRU cache)
│   └── discovery.py                    # Asyncio UDP RPCs (PING, FIND_NODE), bootstrap join
│
├── gossip/                             # Gossip protocol & failure detection (Member 5)
│   ├── models.py                       # PeerNodeRecord, NodeMetrics, GossipMessage
│   ├── metrics.py                      # CPU/RAM collector
│   └── monitor.py                      # GossipMonitor: heartbeats & failure detection
│
├── storage/                            # Persistent storage
│   └── db.py                           # SQLite WAL engine (tasks, audit events, snapshots)
│
├── network/                            # TCP transport layer (Member 2)
│   ├── protocol.py                     # Message framing & protocol constants
│   ├── server.py                       # Async TCP server (asyncio)
│   └── client.py                       # Async TCP client with auto-reconnect
│
├── dashboard/                          # Monitoring & control interfaces (Member 5)
│   ├── web_server.py                   # HTTP server + REST API
│   ├── cli.py                          # Terminal TUI (rich library or ANSI fallback)
│   └── static/
│       └── index.html                  # Single-page Web Dashboard
│
├── tests/                              # Comprehensive test suite (31+ tests)
│   ├── test_tasks.py                   # Task models, serializer & execution tests
│   ├── test_scheduler.py               # Load-based routing tests
│   ├── test_reliability.py             # Lease tracking & recovery tests
│   ├── test_security.py                # Cryptographic signature & tamper tests
│   └── run_all_tests.py                # Master test runner
│
├── examples/                           # Runnable demonstrations
│   ├── task_execution_demo.py          # Compute benchmark demo (primes, matrix, Pi)
│   └── task_distribution_demo.py      # Load routing & worker crash recovery demo
│
└── docs/                               # Subsystem specifications
    └── task_engine_architecture.md     # Architecture spec & integration contracts
```

---

## Installation & Edge Deployment

MeshWeaver is built with **zero external dependencies** and runs natively on lightweight edge devices like Raspberry Pis, minimal Linux containers, and local developer machines.

Install directly as an editable package:
```bash
pip install -e .
```

Optional enhanced extras (for rich terminal rendering and advanced pickling):
```bash
pip install -e .[enhanced]
```

---

## Command-Line Interface (CLI)

MeshWeaver provides a unified command-line tool `meshweaver` (or `python meshweaver_cli.py`):

```bash
# Check local node telemetry and engine status
meshweaver status

# Run standard mathematical benchmarks (Monte Carlo Pi, Prime Sieve, Matrix Multiply)
meshweaver benchmark

# Run the full automated test suite
meshweaver test

# Run the task distribution & crash recovery simulation
meshweaver demo
```

---

## Running Automated Tests

Run the master test runner across all subsystems:
```bash
python tests/run_all_tests.py
```

Or run individual subsystem suites:
```bash
python -m unittest tests/test_tasks.py        # Task engine & serializer
python -m unittest tests/test_scheduler.py    # Load routing & overload guards
python -m unittest tests/test_reliability.py  # Fault recovery & lease watchdog
python -m unittest tests/test_security.py     # Cryptographic signatures & tamper gates
```

---

## Architecture Documentation

Detailed architectural specifications, state machines, and teammate integration contracts are available in:
* [`docs/task_engine_architecture.md`](docs/task_engine_architecture.md)

---

## License
MIT License. Developed for Advanced Python Distributed Systems Engineering.
