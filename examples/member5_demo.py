"""
MeshWeaver Member 5 Demo Launcher
Runs the complete Member 5 Gossip Protocol, Fault Tolerance, and Dashboard demonstration.
"""

import asyncio
import os
import sys

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from examples.gossip_telemetry_demo import run_member5_demo

if __name__ == "__main__":
    asyncio.run(run_member5_demo())
