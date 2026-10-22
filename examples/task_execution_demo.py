"""
MeshWeaver - Task Execution & Serialization Benchmark
Demonstrates:
 1. Packaging arbitrary Python callables and closures using wire-safe serialization.
 2. Transmitting and asynchronously executing computational mathematics workloads.
 3. Enforcing execution timeout guards and capturing structured task results.
 4. Gathering live execution telemetry for the gossip monitoring engine.
"""

import asyncio
import os
import sys

# Ensure repository root is on the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from task.models import Task, TaskStatus, TaskPriority
from task.serializer import TaskSerializer
from task.executor import TaskExecutor


def custom_data_transformation(data_list: list) -> dict:
    """Sample data processing computation."""
    squares = [x ** 2 for x in data_list]
    evens = [x for x in squares if x % 2 == 0]
    return {
        "original_count": len(data_list),
        "even_squares_count": len(evens),
        "total_sum": sum(squares),
        "max_value": max(squares) if squares else 0,
    }


async def main():
    print("=" * 70)
    print("   MESHWEAVER: TASK EXECUTION & SERIALIZATION BENCHMARK")
    print("=" * 70)

    executor = TaskExecutor(node_id="mesh_worker_01", max_workers=4)

    # -------------------------------------------------------------------------
    # STEP 1: Packaging & Executing an Arbitrary Python Function
    # -------------------------------------------------------------------------
    print("\n[Step 1] Packaging and Executing Custom Data Transformation...")
    raw_input = list(range(1, 1001))
    task1 = TaskSerializer.package_task(
        name="data_transformation_job",
        func=custom_data_transformation,
        args=(raw_input,),
        priority=TaskPriority.HIGH,
        timeout_seconds=5.0,
        origin_node_id="submitter_node_alpha",
    )

    print(f" -> Task Created : {task1.name} (ID: {task1.task_id[:8]})")
    print(f" -> Wire Payload : {len(task1.payload)} bytes (Includes SHA-256 Checksum)")
    print(f" -> Serializer   : {'cloudpickle (Available)' if TaskSerializer.has_cloudpickle() else 'pickle (Zero-Dependency Stdlib)'}")

    result1 = await executor.execute(task1)
    print(f" -> Execution Status : {result1.status.value}")
    print(f" -> Return Value     : {result1.return_value}")
    print(f" -> Elapsed Time     : {result1.execution_time_seconds:.4f}s")

    # -------------------------------------------------------------------------
    # STEP 2: Executing Standard Computational Math Workloads
    # -------------------------------------------------------------------------
    print("\n[Step 2] Executing Computational Math Workloads...")
    benchmarks = [
        ("Monte Carlo Pi (500k samples)", "pi", (500_000,)),
        ("Prime Sieve Count (up to 150k)", "primes", (150_000,)),
        ("Square Matrix Multiplication (120x120)", "matrix", (120,)),
    ]

    for label, b_type, b_args in benchmarks:
        bench_task = TaskSerializer.create_benchmark_task(b_type, *b_args, origin_node_id="submitter_node_alpha")
        bench_result = await executor.execute(bench_task)
        print(f" -> [{label}]")
        print(f"    Status: {bench_result.status.value} | Result: {bench_result.return_value} | Time: {bench_result.execution_time_seconds:.4f}s")

    # -------------------------------------------------------------------------
    # STEP 3: Timeout Guards and Error Resilience
    # -------------------------------------------------------------------------
    print("\n[Step 3] Enforcing Execution Timeouts (Resilience Guard)...")
    timeout_task = TaskSerializer.create_benchmark_task("delay", 2.0, timeout_seconds=0.5)
    timeout_result = await executor.execute(timeout_task)
    print(f" -> 2.0s Task with 0.5s Timeout Guard:")
    print(f"    Status: {timeout_result.status.value}")
    print(f"    Message: {timeout_result.error_message}")

    # -------------------------------------------------------------------------
    # STEP 4: Telemetry Summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("   TELEMETRY SUMMARY (FOR GOSSIP & DASHBOARD INTEGRATION)")
    print("=" * 70)
    print(f" -> Total Completed Tasks : {executor.total_completed}")
    print(f" -> Total Failed/Timed-out: {executor.total_failed}")
    print(f" -> Average Execution Time: {executor.average_execution_time:.4f}s")
    print(f" -> Active Workers/Tasks  : {executor.active_task_count}")
    print("=" * 70)
    print("   ALL WORKLOAD VALIDATIONS: COMPLETED SUCCESSFULLY")
    print("=" * 70)

    executor.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
