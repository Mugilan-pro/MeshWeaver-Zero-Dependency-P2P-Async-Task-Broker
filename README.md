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

### 2. Run Interactive Demos

Run the Task Execution & Serialization Benchmark:
```bash
python examples/task_execution_demo.py
```

Run the Decentralized Distribution & Node Failure Recovery Simulation:
```bash
python examples/task_distribution_demo.py
```
