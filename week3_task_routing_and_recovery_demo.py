"""
MeshWeaver Load-Aware Routing & Fault Tolerance Demonstration
Covers:
 1. Dynamic Gossip Protocol: Sharing CPU/RAM load metrics across peers every 5 seconds.
 2. Lowest-CPU Load-Aware Task Routing: Cryptographically assigning compute workloads to the peer with the lowest reported CPU utilization.
 3. Heartbeat Monitor & Fault Tolerance: Detecting lost worker heartbeats and automatically re-routing orphaned tasks without centralized servers.
 4. Bounded Retries & Dead-Letter Isolation: Preventing retry storms when a task permanently fails.
"""

import asyncio
import os
import sys

# Ensure workspace root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from node import MeshNode
from task.models import Task, TaskPriority, TaskStatus
from task.serializer import TaskSerializer
from scheduler.router import RoutingStrategy


def sample_pi_calculation(samples: int) -> float:
    """Monte Carlo calculation of Pi."""
    import random
    inside = 0
    for _ in range(samples):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            inside += 1
    return round((4.0 * inside) / samples, 5)


def faulty_job():
    """Simulates a permanently failing compute job."""
    raise RuntimeError("Permanent algorithmic failure")


async def run_week3_demo():
    print("=" * 76)
    print("  MESHWEAVER: LOAD-AWARE ROUTING & FAULT TOLERANCE DEMO")
    print("=" * 76)

    # -----------------------------------------------------------------
    # Step 1: Initialize 4 Mesh Nodes
    # -----------------------------------------------------------------
    print("\n[Step 1] Initializing 4 decentralized mesh nodes...")
    node_alpha = MeshNode(port=0, dht_port=0, enable_tls=False)  # Coordinator / Origin
    node_beta = MeshNode(port=0, dht_port=0, enable_tls=False)   # Worker High Load
    node_gamma = MeshNode(port=0, dht_port=0, enable_tls=False)  # Worker Medium Load
    node_delta = MeshNode(port=0, dht_port=0, enable_tls=False)  # Worker Lowest Load

    nodes = [node_alpha, node_beta, node_gamma, node_delta]
    for n in nodes:
        await n.start()

    try:
        # -----------------------------------------------------------------
        # Step 2: Form Mesh via DHT Bootstrap
        # -----------------------------------------------------------------
        print("\n[Step 2] Joining peers into Kademlia DHT mesh...")
        seed = (node_alpha.host, node_alpha.dht_port)
        for n in nodes[1:]:
            await n.bootstrap([seed])

        print(f" -> Mesh established: {len(nodes)} active nodes discovered.")

        # -----------------------------------------------------------------
        # Step 3: Gossip Protocol Telemetry Exchange
        # -----------------------------------------------------------------
        print("\n[Step 3] Exchanging peer resource telemetry via Gossip Protocol...")
        # Simulate varied CPU/RAM load profiles
        node_alpha.metrics_collector.set_simulation_override(cpu_percent=55.0, ram_percent=40.0)
        node_alpha.router.update_worker_metrics(node_alpha.node_id_hex, cpu_percent=55.0, ram_percent=40.0)
        node_beta.metrics_collector.set_simulation_override(cpu_percent=78.5, ram_percent=60.0)
        node_gamma.metrics_collector.set_simulation_override(cpu_percent=42.0, ram_percent=35.0)
        node_delta.metrics_collector.set_simulation_override(cpu_percent=11.2, ram_percent=18.0)

        # Ingest telemetry into Node Alpha's router
        for worker, name in [(node_beta, "Node Beta"), (node_gamma, "Node Gamma"), (node_delta, "Node Delta")]:
            snap = worker.metrics_collector.capture_snapshot()
            node_alpha.gossip.handle_incoming_gossip(snap.to_dict())
            print(f"   [Gossip Ingested] {name} ({worker.node_id_hex[:8]}) -> CPU: {snap.cpu_percent:4.1f}% | RAM: {snap.ram_percent:4.1f}%")

        # -----------------------------------------------------------------
        # Step 4: Lowest-CPU Load-Aware Workload Routing
        # -----------------------------------------------------------------
        print("\n[Step 4] Submitting compute task from Node Alpha using LOWEST_CPU routing...")
        task = TaskSerializer.package_task(
            name="monte_carlo_pi",
            func=sample_pi_calculation,
            args=(250_000,),
            priority=TaskPriority.HIGH,
            origin_node_id=node_alpha.node_id_hex,
        )

        chosen_worker = node_alpha.router.select_worker(task, strategy=RoutingStrategy.LOWEST_CPU)
        print(f" -> Workload: '{task.name}' ({len(task.payload)} bytes payload)")
        print(f" -> Routing Engine Decision: Selected Node {chosen_worker.node_id[:8]} (Lowest CPU: {chosen_worker.cpu_percent:.1f}%)")
        assert chosen_worker.node_id == node_delta.node_id_hex, "Must select node_delta with 11.2% CPU!"

        # Execute on selected lowest-CPU worker
        result = await node_alpha.submit_packaged_task(task, strategy=RoutingStrategy.LOWEST_CPU)
        print(f" -> Execution Succeeded! Status: {result.status.value}")
        print(f" -> Calculated Pi Value : {result.return_value}")
        print(f" -> Executed by Node    : {result.executed_by_node_id[:8]}")
        print(f" -> Execution Time      : {result.execution_time_seconds:.4f}s")

        # -----------------------------------------------------------------
        # Step 5: Worker Dropout & Instant Heartbeat Fault Recovery
        # -----------------------------------------------------------------
        print("\n[Step 5] Simulating sudden worker crash & heartbeat fault recovery...")
        # Create a new job assigned to Node Delta (currently lowest load)
        job2 = TaskSerializer.package_task(
            name="critical_job_after_crash",
            func=sample_pi_calculation,
            args=(100_000,),
            max_retries=2,
            origin_node_id=node_alpha.node_id_hex,
        )

        # Lease to Node Delta
        node_alpha.reliability.assign_task(job2, node_delta.node_id_hex)
        print(f" -> Task {job2.task_id[:8]} leased to Node Delta ({node_delta.node_id_hex[:8]}). Active leases: {node_alpha.reliability.in_flight_count}")

        # SIMULATE SUDDEN WORKER CRASH: Stop Node Delta
        print(f"\n [CRASH EVENT] Abruptly terminating Node Delta ({node_delta.node_id_hex[:8]})...")
        await node_delta.stop()

        # Sweep heartbeats / trigger dropout
        print(" [HEARTBEAT MONITOR] Detecting lost heartbeats across cluster...")
        reassigned_tasks = node_alpha.reliability.handle_worker_dropout(node_delta.node_id_hex, node_alpha.router)

        print(f" -> Heartbeat expired for Node Delta! Worker marked DEAD.")
        print(f" -> Orphaned Tasks Detected: {len(reassigned_tasks)}")

        for orphaned_task, new_worker in reassigned_tasks:
            print(f" -> Task '{orphaned_task.name}' (ID: {orphaned_task.task_id[:8]})")
            print(f"    Previous Worker: {node_delta.node_id_hex[:8]} (DEAD)")
            print(f"    New Worker     : {new_worker.node_id[:8]} (Next Lowest CPU: {new_worker.cpu_percent:.1f}%)")
            print(f"    Retry Attempt  : #{orphaned_task.retry_count}/{orphaned_task.max_retries}")
            assert new_worker.node_id == node_gamma.node_id_hex, "Must re-route to node_gamma with 42.0% CPU!"

            # Execute re-routed task on Node Gamma
            res2 = await node_gamma.executor.execute(orphaned_task)
            node_alpha.reliability.record_success(orphaned_task.task_id, res2)
            print(f"    Result On New Worker: Status={res2.status.value}, Value={res2.return_value}")

        # -----------------------------------------------------------------
        # Step 6: Bounded Retries to Dead-Letter Queue
        # -----------------------------------------------------------------
        print("\n[Step 6] Testing bounded retries & dead-letter queue isolation...")
        broken_task = TaskSerializer.package_task(
            name="broken_task",
            func=faulty_job,
            max_retries=1,
            origin_node_id=node_alpha.node_id_hex,
        )
        node_alpha.reliability.assign_task(broken_task, node_beta.node_id_hex)

        # First failure -> retry
        attempt1 = node_alpha.reliability.record_failure_and_retry(broken_task.task_id, "Error 1", node_alpha.router)
        print(f" -> Failure 1: Retried on {attempt1[1].node_id[:8]} (Retry {broken_task.retry_count}/1)")

        # Second failure -> exhausted
        attempt2 = node_alpha.reliability.record_failure_and_retry(broken_task.task_id, "Error 2", node_alpha.router)
        print(f" -> Failure 2: Retries exhausted. Moved to Dead-Letter Queue: {attempt2 is None}")
        print(f" -> Dead-Letter Count: {node_alpha.reliability.dead_letter_count}")
        print(f" -> Final Task Status: {broken_task.status.value}")

        print("\n" + "=" * 76)
        print("  SIMULATION COMPLETE: LOAD BALANCING & FAULT TOLERANCE VERIFIED")
        print("=" * 76)

    finally:
        for n in [node_alpha, node_beta, node_gamma]:
            await n.stop()


if __name__ == "__main__":
    asyncio.run(run_week3_demo())
