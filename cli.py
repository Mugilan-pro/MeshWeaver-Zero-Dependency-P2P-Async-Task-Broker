"""
MeshWeaver CLI Utility
Command-line interface to launch mesh nodes, submit compute workloads,
view real-time network topology dashboards, and run cluster audits.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

from node import MeshNode
from task.serializer import TaskSerializer
from scheduler.router import RoutingStrategy
from dashboard.cli import MeshDashboard


def parse_args():
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="MeshWeaver: Zero-Dependency Decentralized P2P Async Task Broker",
    )
    subparsers = parser.add_subparsers(dest="command", help="Sub-commands")

    # Command: start
    start_p = subparsers.add_parser("start", help="Start a MeshWeaver node")
    start_p.add_argument("--host", default="127.0.0.1", help="Binding host address (default: 127.0.0.1)")
    start_p.add_argument("--port", type=int, default=8000, help="TCP/TLS task port (default: 8000)")
    start_p.add_argument("--dht-port", type=int, default=8001, help="UDP DHT discovery port (default: 8001)")
    start_p.add_argument("--bootstrap", help="Bootstrap seed endpoint host:port (e.g. 127.0.0.1:8001)")
    start_p.add_argument("--no-tls", action="store_true", help="Disable TLS transport encryption")
    start_p.add_argument("--no-signatures", action="store_true", help="Disable cryptographic signature requirement")

    # Command: run-audit (Mid-Project & Final Reviews)
    audit_p = subparsers.add_parser("audit", help="Run multi-node cluster verification audit")
    audit_p.add_argument("--nodes", type=int, default=10, help="Number of nodes to spin up (default: 10)")

    # Command: demo-week3 (Week 3 Task Routing & Fault Recovery)
    subparsers.add_parser("demo-week3", help="Run Week 3 Lowest-CPU routing and worker dropout recovery demo")

    # Command: demo-week4 (Week 4 TLS Security & Rich Dashboard)
    subparsers.add_parser("demo-week4", help="Run Week 4 TLS encryption, Ed25519 signatures & Rich dashboard demo")

    return parser.parse_args()


async def cmd_start(args):
    enable_tls = not args.no_tls
    require_signatures = not args.no_signatures

    node = MeshNode(
        host=args.host,
        port=args.port,
        dht_port=args.dht_port,
        enable_tls=enable_tls,
        require_signatures=require_signatures,
    )
    await node.start()

    if args.bootstrap:
        b_host, b_port = args.bootstrap.split(":")
        print(f"Connecting to bootstrap node {b_host}:{b_port}...")
        joined = await node.bootstrap([(b_host, int(b_port))])
        print(f"Bootstrap status: {'SUCCESS' if joined else 'FAILED'}")

    print(f"\nMeshWeaver Node {node.node_id_hex[:8]} is running. Press Ctrl+C to terminate.\n")

    try:
        while True:
            await asyncio.sleep(2.0)
            node.render_dashboard()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nStopping node...")
        await node.stop()


def main():
    args = parse_args()
    if not args.command or args.command == "start":
        asyncio.run(cmd_start(args))
    elif args.command == "demo-week3":
        from examples.week3_task_routing_and_recovery_demo import run_week3_demo
        asyncio.run(run_week3_demo())
    elif args.command == "demo-week4":
        from examples.week4_security_and_dashboard_demo import run_week4_demo
        asyncio.run(run_week4_demo())
    elif args.command == "audit":
        from examples.network_audit_demo import run_network_audit
        asyncio.run(run_network_audit(num_nodes=args.nodes))


if __name__ == "__main__":
    main()
