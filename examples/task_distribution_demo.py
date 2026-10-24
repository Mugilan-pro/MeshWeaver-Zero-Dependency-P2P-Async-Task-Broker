"""
MeshWeaver - Task Distribution and Execution Engine Demo
Demonstrates:
 1. Workload packaging & SHA-256 binary integrity verification.
 2. Non-blocking asynchronous task execution with timeout enforcement.
 3. Lowest CPU-load workload routing across decentralized mesh nodes.
 4. Automatic fault recovery: Detecting node dropouts and reassignment.
"""

import asyncio
import os
import sys

# Ensure repository root is on Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from task.models import Task, TaskStatus, TaskPriority
from task.serializer import TaskSerializer
from task.executor import TaskExecutor
from scheduler.router import TaskRouter, RoutingStrategy, WorkerNodeInfo
from scheduler.reliability import ReliabilityManager


def sample_scientific_simulation(iterations: int) -> float:
    """Simulates a CPU-bound mathematical calculation."""
    import math
    acc = 0.0
    for i in range(1, iterations + 1):
        acc += math.sqrt(i) * math.sin(i)
    return round(acc, 4)


async def run_simulation():
    print("=" * 70)
    print("  MESHWEAVER: DECENTRALIZED TASK BROKER - CORE ENGINE DEMO")
    print("=" * 70)

    # -------------------------------------------------------------
    # 1. Packaging & Serialization
    # -------------------------------------------------------------
    print("\n[Step 1] Packaging compute workload with SHA-256 integrity tag...")
    task = TaskSerializer.package_task(
        name="scientific_simulation",
        func=sample_scientific_simulation,
        args=(150_000,),
        priority=TaskPriority.HIGH,
        timeout_seconds=5.0,
        origin_node_id="origin_node_alpha",
    )
    print(f" -> Task ID       : {task.task_id[:8]}... ({task.name})")
    print(f" -> Wire Payload  : {len(task.payload)} bytes")
    print(f" -> Engine Support: Cloudpickle={TaskSerializer.has_cloudpickle()} (Fallback=Stdlib)")

    # -------------------------------------------------------------
    # 2. Asynchronous Execution via TaskExecutor
    # -------------------------------------------------------------
    print("\n[Step 2] Executing workload asynchronously in isolated worker pool...")
    executor = TaskExecutor(node_id="worker_local_01")
    result = await executor.execute(task)

    print(f" -> Outcome Status : {result.status.value}")
    print(f" -> Return Value   : {result.return_value}")
    print(f" -> Execution Time : {result.execution_time_seconds:.4f}s")
    print(f" -> Executed On    : {result.executed_by_node_id}")

    # -------------------------------------------------------------
    # 3. Load-Aware Task Routing (Lowest CPU Load)
    # -------------------------------------------------------------
    print("\n[Step 3] Initializing TaskRouter & Ingesting Peer Metrics...")
    router = TaskRouter(local_node_id="worker_local_01", default_strategy=RoutingStrategy.LOWEST_CPU)

    # Simulated peer nodes with live telemetry
    peer_nodes = [
        WorkerNodeInfo(node_id="mesh_node_01", host="10.0.0.1", port=9001, cpu_percent=78.2, ram_percent=60.0, active_tasks=3),
        WorkerNodeInfo(node_id="mesh_node_02", host="10.0.0.2", port=9002, cpu_percent=45.0, ram_percent=35.0, active_tasks=1),
        WorkerNodeInfo(node_id="mesh_node_03", host="10.0.0.3", port=9003, cpu_percent=12.4, ram_percent=18.0, active_tasks=0),
        WorkerNodeInfo(node_id="mesh_node_04", host="10.0.0.4", port=9004, cpu_percent=55.6, ram_percent=42.0, active_tasks=2),
    ]

    for node in peer_nodes:
        router.register_or_update_worker(node)
        print(f"   [Peer Registered] {node.node_id} -> CPU: {node.cpu_percent:4.1f}% | Active Tasks: {node.active_tasks}")

    benchmark_task = TaskSerializer.create_benchmark_task("primes", 120_000, origin_node_id="worker_local_01")
    selected_worker = router.select_worker(benchmark_task)

    print(f"\n -> Workload Routing Decision for '{benchmark_task.name}':")
    print(f"    Assigned Worker: {selected_worker.node_id} ({selected_worker.host}:{selected_worker.port})")
    print(f"    Worker CPU Load: {selected_worker.cpu_percent:.1f}% (Lowest across mesh)")

    # -------------------------------------------------------------
    # 4. Fault Tolerance: Node Drop & Instant Reassignment
    # -------------------------------------------------------------
    print("\n[Step 4] Simulating Worker Failure & Resilient Reassignment...")
    reliability = ReliabilityManager(default_max_retries=3)

    # Assign task to the chosen node
    reliability.assign_task(benchmark_task, selected_worker.node_id)
    print(f" -> Task {benchmark_task.task_id[:8]} leased to {selected_worker.node_id}. Active leases: {reliability.in_flight_count}")

    print(f"\n [ALERT] Heartbeat lost for {selected_worker.node_id}! Node marked DEAD.")
    recovered_tasks = reliability.handle_worker_dropout(selected_worker.node_id, router)

    for orphaned_task, new_target in recovered_tasks:
        print(f" -> Recovered Task     : {orphaned_task.name} (Retry #{orphaned_task.retry_count})")
        print(f" -> Reassigned Worker  : {new_target.node_id} (CPU: {new_target.cpu_percent:.1f}%)")

    print("\n" + "=" * 70)
    print("  SIMULATION COMPLETE: ALL SUBSYSTEMS FULLY OPERATIONAL")
    print("=" * 70)

    executor.shutdown()


if __name__ == "__main__":
    asyncio.run(run_simulation())
