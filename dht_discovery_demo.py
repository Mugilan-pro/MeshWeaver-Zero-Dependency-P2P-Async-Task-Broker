"""
Interactive demo of Member 3 DHT features:
1. NodeID generation and XOR distance calculation
2. Asynchronous UDP ping/pong and RTT latency measurement (Week 1)
3. Routing table k-buckets and closest node lookup (Week 2)
4. Dynamic bootstrap joining into a local cluster (Week 2)
"""

import asyncio
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dht.node_id import NodeContact, NodeID
from dht.routing import RoutingTable
from dht.discovery import DHTDiscoveryNode


async def demo_node_id_math():
    print("\n--- 1. Node IDs and XOR Distance ---")
    id1 = NodeID.generate()
    id2 = NodeID.generate()
    id3 = NodeID.from_endpoint("127.0.0.1", 9000)

    print(f"Node 1 ID : {id1.hex_value} ({id1.short_hex(8)})")
    print(f"Node 2 ID : {id2.hex_value} ({id2.short_hex(8)})")
    print(f"Node 3 ID : {id3.hex_value} (hashed from 127.0.0.1:9000)")

    d12 = id1.xor_distance(id2)
    d21 = id2.xor_distance(id1)
    d13 = id1.xor_distance(id3)
    d23 = id2.xor_distance(id3)

    print(f"\nXOR distance d(1, 2): {d12}")
    print(f"Symmetry check d(1, 2) == d(2, 1): {d12 == d21}")
    print(f"Triangle inequality d(1, 3) <= d(1, 2) + d(2, 3): {d13 <= d12 + d23}")
    print(f"Shared prefix bits between 1 and 2: {id1.shared_prefix_length(id2)}")
    print(f"K-bucket index for Node 2 in Node 1's table: {id1.bucket_index(id2)}")


async def demo_udp_ping():
    print("\n--- 2. Week 1: UDP Ping / Pong RPC ---")
    node_a = DHTDiscoveryNode(host="127.0.0.1", port=0)
    node_b = DHTDiscoveryNode(host="127.0.0.1", port=0)

    await node_a.start()
    await node_b.start()

    print(f"Started Node A on {node_a.endpoint_str} (ID: {node_a.node_id.short_hex(6)})")
    print(f"Started Node B on {node_b.endpoint_str} (ID: {node_b.node_id.short_hex(6)})")

    print(f"Node A sending PING to Node B...")
    success, rtt_ms, peer_id = await node_a.ping(node_b.endpoint)

    if success:
        print(f"Got PONG from Node B in {rtt_ms:.2f} ms (verified peer ID: {peer_id.short_hex(6)})")
        print(f"Node A known peers: {node_a.routing_table.total_contacts}")
        print(f"Node B known peers: {node_b.routing_table.total_contacts} (learned Node A from ping)")
    else:
        print("Ping failed or timed out.")

    await node_a.stop()
    await node_b.stop()


async def demo_routing_table():
    print("\n--- 3. Week 2: K-Bucket Closest Peer Search ---")
    local_id = NodeID.generate()
    table = RoutingTable(local_node_id=local_id, k=4)
    print(f"Created routing table for local node {local_id.short_hex(6)}")

    # Add dummy contacts
    for i in range(10):
        c = NodeContact(node_id=NodeID.generate(), host="127.0.0.1", port=8000 + i, rtt_ms=float(i * 1.2))
        table.add_or_update(c)

    print(f"Added 10 peers across {len(table.get_populated_buckets())} buckets")

    target = NodeID.generate()
    print(f"Finding 3 closest nodes to random target {target.short_hex(6)}:")
    closest = table.find_closest_nodes(target_id=target, count=3)
    for idx, node in enumerate(closest, 1):
        dist = node.node_id.xor_distance(target)
        print(f"  {idx}. {node.node_id.short_hex(6)} at {node.endpoint_str} (distance: {dist:#010x}...)")


async def demo_bootstrap():
    print("\n--- 4. Week 2: Dynamic Bootstrap Cluster ---")
    # Start 4 nodes
    nodes = [DHTDiscoveryNode(host="127.0.0.1", port=0) for _ in range(4)]
    for n in nodes:
        await n.start()

    bootstrap_addr = nodes[0].endpoint
    print(f"Bootstrap node online at {nodes[0].endpoint_str}")

    # Other 3 nodes join via bootstrap
    for idx, node in enumerate(nodes[1:], 1):
        ok = await node.bootstrap([bootstrap_addr])
        status = "joined" if ok else "failed"
        print(f"Worker {idx} ({node.endpoint_str}) {status} -> peers in table: {node.routing_table.total_contacts}")

    for n in nodes:
        await n.stop()


async def main():
    print("MeshWeaver DHT Discovery Demo (Member 3)")
    print("=" * 50)
    await demo_node_id_math()
    await demo_udp_ping()
    await demo_routing_table()
    await demo_bootstrap()
    print("\nDemo finished successfully.")


if __name__ == "__main__":
    asyncio.run(main())
