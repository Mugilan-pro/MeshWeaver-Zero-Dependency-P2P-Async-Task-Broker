"""
MeshWeaver Demo Launcher
Redirects to the professional workload distribution demo.
"""

import asyncio
import os
import sys

# Ensure repository root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from examples.task_distribution_demo import run_simulation

if __name__ == "__main__":
    asyncio.run(run_simulation())
