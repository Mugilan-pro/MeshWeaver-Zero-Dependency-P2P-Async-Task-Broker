"""
MeshWeaver Demo Launcher
Redirects to the professional workload distribution demo.
"""

import asyncio
from examples.task_distribution_demo import run_simulation

if __name__ == "__main__":
    asyncio.run(run_simulation())
