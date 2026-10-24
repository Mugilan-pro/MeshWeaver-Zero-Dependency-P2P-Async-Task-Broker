"""
Mid-Project Review Network Audit: 10-Node Dynamic Mesh Discovery.
Runs 10 local DHT instances on ports 9001-9010.
Node 1 acts as the bootstrap seed. Nodes 2 through 10 have zero hardcoded peer lists
and must discover the rest of the network purely through Kademlia RPCs.
"""

import asyncio
import os
import sys
import time
from typing import List

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dht.discovery import DHTDiscoveryNode
from dht.node_id import NodeID


async def main():
    print("MeshWeaver Mid-Project Review: 10-Node Network Audit")
    print("Testing decentralized discovery without hardcoded IPs...")
    print("=" * 65)

    NODE_COUNT = 10
    BASE_PORT = 9001
    HOST = "127.0.0.1"

    nodes: List[DHTDiscoveryNode] = []

    # 1. Start bootstrap gateway
    bootstrap = DHTDiscoveryNode(
        host=HOST,
        port=BASE_PORT,
        node_id=NodeID.from_endpoint(HOST, BASE_PORT),
    )
    await bootstrap.start()
    nodes.append(bootstrap)
    print(f"Bootstrap node started on {bootstrap.endpoint_str} (ID: {bootstrap.node_id.short_hex(6)})")

    # 2. Start worker nodes
    for i in range(2, NODE_COUNT + 1):
        port = BASE_PORT + i - 1
        n = DHTDiscoveryNode(
            host=HOST,
            port=port,
            node_id=NodeID.from_endpoint(HOST, port),
        )
        await n.start()
        nodes.append(n)
        print(f"Started Node {i:2d} on {n.endpoint_str} (0 peers initially)")

    # 3. Join nodes via bootstrap
    print(f"\nJoining nodes via bootstrap seed ({HOST}:{BASE_PORT})...")
    start = time.perf_counter()

    for idx, node in enumerate(nodes[1:], 2):
        joined = await node.bootstrap([(HOST, BASE_PORT)])
        print(f"  Node {idx:2d} bootstrap status: {'OK' if joined else 'FAIL'} (known peers: {node.routing_table.total_contacts})")

    # 4. Give nodes a moment to do mutual neighborhood lookups
    print("Running iterative neighborhood lookups to ensure full convergence...")
    for node in nodes:
        await node.iterative_find_node(node.node_id)
        await node.iterative_find_node(NodeID.generate())

    duration = time.perf_counter() - start
    print(f"Discovery converged in {duration:.3f} seconds.\n")

    # 5. Connectivity Matrix
    print("Mesh Connectivity Matrix (OK = peer discovered):")
    print("-" * 65)
    header = "Node    | Port | ID       | Peers | " + " ".join(f"N{i:02d}" for i in range(1, NODE_COUNT + 1))
    print(header)
    print("-" * 65)

    all_ids = [n.node_id for n in nodes]
    fully_connected = True

    for i, node in enumerate(nodes, 1):
        known = {c.node_id for c in node.routing_table.get_all_contacts()}
        cells = []
        for j, target_id in enumerate(all_ids, 1):
            if i == j:
                cells.append(" -- ")
            elif target_id in known:
                cells.append(" OK ")
            else:
                cells.append("  . ")
                fully_connected = False

        print(f"Node {i:02d} | {node.port} | {node.node_id.short_hex(6)} |  {len(known):2d}   | " + " ".join(cells))

    print("-" * 65)
    print(f"Audit result: {'PASS (Fully connected mesh)' if fully_connected else 'PARTIAL'}")
    print(f"Total nodes: {len(nodes)} | Hardcoded peer lists: None")

    # Clean shutdown
    print("\nShutting down nodes...")
    for n in nodes:
        await n.stop()
    print("All sockets closed.")


if __name__ == "__main__":
    asyncio.run(main())
