#!/usr/bin/env python3
"""
MeshWeaver Command-Line Interface (CLI)
Zero-dependency administrative utility for orchestrating nodes, submitting tasks,
viewing the live topology dashboard, and launching validation demos.
"""

import argparse
import asyncio
import os
import sys

# Ensure repository root is on Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config import DEFAULT_HOST, DEFAULT_PORT, ENABLE_TLS
from dashboard.cli import DashboardEventLogger, RichTerminalDashboard
from gossip.monitor import GossipMonitor
from mesh.node import MeshNode
from scheduler.reliability import ReliabilityManager
from scheduler.router import RoutingStrategy, TaskRouter
from security.tls import ensure_node_certificates
from task.models import TaskPriority
from task.serializer import TaskSerializer


def cmd_certs(args):
    """Generates X.509 self-signed TLS certificates for a node."""
    print(f"Generating TLS certificates for node '{args.node_id}'...")
    cert_path, key_path = ensure_node_certificates(args.node_id, cert_dir=args.cert_dir)
    print(f"✓ Certificate: {cert_path}")
    print(f"✓ Private Key: {key_path}")


def cmd_dashboard(args):
    """Renders a snapshot or live-updating cluster dashboard."""
    monitor = GossipMonitor(local_node_id=args.node_id, host=args.host, port=args.port)
    router = TaskRouter(local_node_id=args.node_id)
    reliability = ReliabilityManager()

    if args.peer:
        for p in args.peer:
            nid, port = p.split(":")
            peer_record = monitor.add_peer(nid, host=args.host, port=int(port))
            router.update_worker_metrics(nid, cpu_percent=20.0, host=args.host, port=int(port))

    dash = RichTerminalDashboard(monitor=monitor, router=router, reliability=reliability)
    if args.live:
        print(f"Starting live dashboard for node '{args.node_id}' (Ctrl+C to exit)...")
        asyncio.run(dash.run_live(refresh_seconds=args.refresh))
    else:
        dash.print_snapshot()


def cmd_demo(args):
    """Runs milestone demonstrations."""
    demo_name = args.name.lower()
    if demo_name in ("week1", "week2", "week1_2", "week1-2"):
        from examples.week1_2_gossip_demo import main as demo_week1_2
        asyncio.run(demo_week1_2())
    elif demo_name in ("week3", "routing", "fault_tolerance"):
        from examples.week3_routing_demo import run_week3_demo
        asyncio.run(run_week3_demo())
    elif demo_name in ("week4", "security", "dashboard"):
        from examples.week4_security_demo import run_week4_demo
        asyncio.run(run_week4_demo())
    elif demo_name in ("full", "mesh", "cluster", "all"):
        from examples.week3_4_full_mesh_demo import run_full_mesh_demo
        asyncio.run(run_full_mesh_demo())
    elif demo_name in ("member5", "m5"):
        from examples.member5_week3_4_demo import run_member5_week3_4_demo
        asyncio.run(run_member5_week3_4_demo())
    elif demo_name in ("member4", "m4"):
        from examples.task_distribution_demo import run_simulation
        asyncio.run(run_simulation())
    else:
        print(f"Unknown demo: '{args.name}'. Options: week1_2, week3, week4, member5, member4, full")


def main():
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="MeshWeaver: Decentralized Zero-Dependency P2P Task Broker",
    )
    subparsers = parser.add_subparsers(dest="subcommand", help="Subcommands")

    # certs
    p_cert = subparsers.add_parser("certs", help="Generate TLS certificates")
    p_cert.add_argument("--node-id", default="node_alpha", help="Node identifier")
    p_cert.add_argument("--cert-dir", default=None, help="Directory to store certificates")
    p_cert.set_defaults(func=cmd_certs)

    # dashboard
    p_dash = subparsers.add_parser("dashboard", help="Render cluster topology dashboard")
    p_dash.add_argument("--node-id", default="local_hub", help="Local node identifier")
    p_dash.add_argument("--host", default=DEFAULT_HOST, help="Host interface")
    p_dash.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port")
    p_dash.add_argument("--peer", action="append", help="Peer format: <node_id>:<port>")
    p_dash.add_argument("--live", action="store_true", help="Run live-updating UI loop")
    p_dash.add_argument("--refresh", type=float, default=1.0, help="Refresh interval (seconds)")
    p_dash.set_defaults(func=cmd_dashboard)

    # demo
    p_demo = subparsers.add_parser("demo", help="Run project milestone demonstrations")
    p_demo.add_argument("name", choices=["week1_2", "week3", "week4", "member5", "member4", "full"], help="Demo to run")
    p_demo.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    if not args.subcommand:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
