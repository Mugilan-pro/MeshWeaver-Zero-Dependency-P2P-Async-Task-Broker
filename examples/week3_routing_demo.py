"""
MeshWeaver Week 3 Milestone Demo: Task Routing & Fault Tolerance
Demonstrates:
 1. Task Routing Algorithm: Routes submitted compute workloads to the specific
    node with the lowest reported CPU load across the decentralized mesh.
 2. Dynamic Metric Adaptation: Route decisions adapt in real-time as peer loads fluctuate.
 3. Heartbeat Failure Detection: Continuous monitoring flags dropped peers as SUSPECT then DEAD.
 4. Automatic Fault Recovery: If an assigned worker dies mid-flight, the lost heartbeat
    triggers instant reassignment to the next healthiest worker with zero data loss.
 5. Bounded Retries & Dead-Letter Queue (DLQ): Prevents cascading failures.
"""

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dashboard.cli import RichTerminalDashboard, DashboardEventLogger
from gossip.models import NodeMetrics, PeerHealthState
from gossip.monitor import GossipMonitor
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter, WorkerNodeInfo
from task.models import Task, TaskPriority, TaskStatus
from task.serializer import TaskSerializer


async def run_week3_demo():
    print("=" * 78)
    print("      MESHWEAVER WEEK 3 DEMO: TASK ROUTING & FAULT TOLERANCE")
    print("=" * 78)

    # -------------------------------------------------------------------------
    # Part 1: Task Routing (Lowest CPU Load Algorithm)
    # -------------------------------------------------------------------------
    print("\n>>> [1] Workload Routing Algorithm (Lowest Reported CPU Load)")
    print("-" * 78)

    router = TaskRouter(local_node_id="gateway_node", default_strategy=RoutingStrategy.LOWEST_CPU)

    # Cluster of 4 workers with varied telemetry profiles
    workers = [
        WorkerNodeInfo(node_id="worker_alpha", host="192.168.1.11", port=8001, cpu_percent=82.5, ram_percent=70.0, active_tasks=3),
        WorkerNodeInfo(node_id="worker_beta", host="192.168.1.12", port=8002, cpu_percent=14.0, ram_percent=22.0, active_tasks=0),
        WorkerNodeInfo(node_id="worker_gamma", host="192.168.1.13", port=8003, cpu_percent=48.0, ram_percent=55.0, active_tasks=1),
        WorkerNodeInfo(node_id="worker_delta", host="192.168.1.14", port=8004, cpu_percent=91.0, ram_percent=85.0, active_tasks=5),
    ]

    print("Current Cluster Load Telemetry:")
    for w in workers:
        router.register_or_update_worker(w)
        print(f"  • {w.node_id:14} ({w.host}:{w.port}) -> CPU: {w.cpu_percent:4.1f}% | RAM: {w.ram_percent:4.1f}% | Active Tasks: {w.active_tasks}")

    # Submit Task A
    task_a = TaskSerializer.create_benchmark_task("primes", 50_000, origin_node_id="gateway_node")
    chosen_worker = router.select_worker(task_a, strategy=RoutingStrategy.LOWEST_CPU)

    print(f"\nWorkload Routing Decision for '{task_a.name}' (ID: {task_a.task_id[:8]}):")
    print(f"  ✓ Assigned Target Worker: {chosen_worker.node_id}")
    print(f"  ✓ Reported CPU Load     : {chosen_worker.cpu_percent:.1f}% (Lowest reported across cluster)")
    assert chosen_worker.node_id == "worker_beta", "Algorithm failed to select lowest CPU worker!"

    # Simulate dynamic load change
    print("\nDynamic Load Shift: worker_beta spikes to 95.0% CPU, worker_gamma drops to 8.0% CPU...")
    router.update_worker_metrics("worker_beta", cpu_percent=95.0)
    router.update_worker_metrics("worker_gamma", cpu_percent=8.0)

    task_b = TaskSerializer.create_benchmark_task("pi", 100_000, origin_node_id="gateway_node")
    new_worker = router.select_worker(task_b, strategy=RoutingStrategy.LOWEST_CPU)
    print(f"Subsequent Workload Routing for '{task_b.name}':")
    print(f"  ✓ Re-evaluated Target  : {new_worker.node_id} (CPU: {new_worker.cpu_percent:.1f}%)")
    assert new_worker.node_id == "worker_gamma"

    # -------------------------------------------------------------------------
    # Part 2: Fault Tolerance (Heartbeat Monitors & Dropout Reassignment)
    # -------------------------------------------------------------------------
    print("\n>>> [2] Heartbeat Failure Detection & Automatic Task Recovery")
    print("-" * 78)

    monitor = GossipMonitor(local_node_id="gateway_node", heartbeat_timeout=2.0)
    reliability = ReliabilityManager(default_max_retries=3)

    # Wire monitor directly to router and reliability manager
    monitor.sync_with_task_router(router)
    monitor.sync_with_reliability_manager(reliability, router)

    event_logger = DashboardEventLogger()
    dashboard = RichTerminalDashboard(monitor=monitor, router=router, reliability=reliability, event_logger=event_logger)

    p_alpha = monitor.add_peer("worker_alpha", port=8001)
    p_gamma = monitor.add_peer("worker_gamma", port=8003)

    critical_task = TaskSerializer.create_benchmark_task("matrix", 80, origin_node_id="gateway_node")
    reliability.assign_task(critical_task, "worker_gamma")
    event_logger.log("ROUTING", f"Leased task '{critical_task.name}' to worker_gamma (Attempt 1/3)")
    print(f"1. Leased task '{critical_task.name}' ({critical_task.task_id[:8]}) to worker_gamma.")
    print(f"   Active cluster leases: {reliability.in_flight_count}")

    print("\n2. Simulating worker_gamma hardware failure / network partition...")
    p_gamma.last_heartbeat_time = time.time() - 3.0  # Exceeds 2.0s heartbeat cutoff

    print("3. Failure detector evaluates heartbeat health...")
    newly_suspect, newly_dead = monitor.check_peer_health()
    print(f"   [ALERT] Dropped heartbeats detected for: {newly_dead}")
    assert "worker_gamma" in newly_dead
    assert p_gamma.health_state == PeerHealthState.DEAD

    # Verify task was re-routed automatically
    assignment = reliability._active_leases.get(critical_task.task_id)
    print("\n4. Reliability Manager Dropout Recovery Execution:")
    if assignment:
        print(f"   ✓ Captured Orphaned Task : {assignment.task.name} ({assignment.task.task_id[:8]})")
        print(f"   ✓ Reassigned To Worker   : {assignment.worker_node_id}")
        print(f"   ✓ Advanced Retry Attempt : {assignment.task.retry_count}/{assignment.task.max_retries}")
        print(f"   ✓ New Task Status        : {assignment.task.status.value}")

    assert critical_task.retry_count == 1
    assert critical_task.status == TaskStatus.SCHEDULED

    # -------------------------------------------------------------------------
    # Part 3: Bounded Retry Limit & Dead-Letter Queue
    # -------------------------------------------------------------------------
    print("\n>>> [3] Bounded Retry Enforcement & Dead-Letter Queue (DLQ)")
    print("-" * 78)
    failing_task = Task(name="corrupted_job", payload=b"", max_retries=1, origin_node_id="gateway_node")
    reliability.assign_task(failing_task, "worker_alpha")
    print(f"Initial lease: '{failing_task.name}' on worker_alpha (Max Retries: 1)")

    # Failure 1
    r1 = reliability.record_failure_and_retry(failing_task.task_id, "Memory corruption in worker pool", router)
    print(f"Attempt 1 failed -> Retried: {r1 is not None} (Retry count: {failing_task.retry_count}/1)")

    # Failure 2 -> Exceeds max retries
    r2 = reliability.record_failure_and_retry(failing_task.task_id, "Segmentation fault", router)
    print(f"Attempt 2 failed -> Exceeded bounds. Result: {r2} (Status: {failing_task.status.value})")
    print(f"DLQ Count: {reliability.dead_letter_count} task(s) permanently isolated in Dead-Letter Queue.")
    assert failing_task.status == TaskStatus.FAILED
    assert reliability.dead_letter_count == 1

    print("\n" + "=" * 78)
    print("      WEEK 3 MILESTONE FULLY VERIFIED (ALL SPECIFICATIONS MET)")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(run_week3_demo())
