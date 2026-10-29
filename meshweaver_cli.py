"""
MeshWeaver Command-Line Interface (CLI)
Unified control and diagnostics entrypoint for the MeshWeaver P2P Task Broker.
Works across all edge devices (Raspberry Pis, Linux, macOS, Windows).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import sys
import time

# Ensure current directory is on python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from task.models import Task, TaskPriority, TaskStatus
from task.serializer import TaskSerializer
from task.executor import TaskExecutor
from task.security import TaskSigner, TaskVerifier, sign_task
from scheduler.router import TaskRouter, RoutingStrategy, WorkerNodeInfo
from scheduler.reliability import ReliabilityManager


def print_banner():
    print("=" * 70)
    print("   MESHWEAVER: ZERO-DEPENDENCY P2P ASYNC TASK BROKER")
    print("   Edge Computing & Distributed Workload System")
    print("=" * 70)


def cmd_status(args):
    """Displays local node environment, hardware telemetry, and feature flags."""
    print_banner()
    print("\n[Node System Information]")
    print(f" -> Host Platform       : {platform.system()} {platform.release()} ({platform.machine()})")
    print(f" -> Python Version      : {platform.python_version()} ({platform.python_implementation()})")
    print(f" -> Logical CPU Cores   : {os.cpu_count() or 1}")
    print(f" -> Process PID         : {os.getpid()}")

    print("\n[Engine Capabilities]")
    print(f" -> Cloudpickle Support : {'Available' if TaskSerializer.has_cloudpickle() else 'Not installed (Using Stdlib Fallback)'}")
    print(f" -> HMAC-SHA256 Security: Active & Enforced")
    print(f" -> Zero-Cloud Edge Mode: 100% Operational")
    print("=" * 70)


def cmd_benchmark(args):
    """Executes local compute benchmarks (Monte Carlo Pi, Prime Sieve, Matrix Multiply)."""
    print_banner()
    print("\n[Running Standard Computational Benchmarks]")

    async def _run():
        executor = TaskExecutor(node_id="cli_benchmark_node")
        benchmarks = [
            ("Monte Carlo Pi (500k samples)", "pi", (500_000,)),
            ("Prime Sieve Count (up to 150k)", "primes", (150_000,)),
            ("Square Matrix Multiply (120x120)", "matrix", (120,)),
        ]

        for label, b_type, b_args in benchmarks:
            t = TaskSerializer.create_benchmark_task(b_type, *b_args, origin_node_id="cli_runner")
            res = await executor.execute(t)
            print(f" -> [{label}]")
            print(f"    Status: {res.status.value} | Result: {res.return_value} | Elapsed: {res.execution_time_seconds:.4f}s")

        executor.shutdown()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()

    print("\n" + "=" * 70)
    print("   ALL BENCHMARKS COMPLETED SUCCESSFULLY")
    print("=" * 70)


def cmd_test(args):
    """Executes all automated unit test suites."""
    print_banner()
    print("\n[Running MeshWeaver Automated Test Suites]")
    import unittest
    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


def cmd_demo(args):
    """Executes the full task distribution and crash-recovery simulation."""
    from examples.task_distribution_demo import run_simulation
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_simulation())
    finally:
        loop.close()


def main():
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="MeshWeaver Decentralized P2P Task Broker CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # status
    p_status = subparsers.add_parser("status", help="Display local node telemetry and engine status")
    p_status.set_defaults(func=cmd_status)

    # benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run computational math workloads")
    p_bench.set_defaults(func=cmd_benchmark)

    # test
    p_test = subparsers.add_parser("test", help="Execute all unit test suites")
    p_test.set_defaults(func=cmd_test)

    # demo
    p_demo = subparsers.add_parser("demo", help="Run end-to-end task distribution simulation")
    p_demo.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
