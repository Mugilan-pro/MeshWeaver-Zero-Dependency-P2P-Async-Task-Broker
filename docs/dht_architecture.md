# MeshWeaver DHT & Peer Discovery Subsystem

## Member 3: Weeks 1 & 2 Documentation

### Overview
This subsystem provides decentralized peer discovery for the MeshWeaver P2P task broker using the **Kademlia Distributed Hash Table (DHT)** algorithm. It eliminates the need for a central registry server (like Redis or ZooKeeper) or hardcoded IP address lists.

---

### Core Components

#### 1. Node IDs & XOR Distance (`dht/node_id.py`)
- **NodeID**: 160-bit integer identifier in the range $[0, 2^{160}-1]$. Can be generated randomly or derived deterministically by hashing an IP/port or string using SHA-1.
- **XOR Distance Metric**:
  $$d(x, y) = x \oplus y$$
  The XOR metric has useful mathematical properties:
  - $d(x, y) = 0 \iff x = y$ (identity)
  - $d(x, y) = d(y, x)$ (symmetry)
  - $d(x, z) \leq d(x, y) \oplus d(y, z)$ (triangle inequality)
- **K-Bucket Index**: The bucket index for a target node is computed directly from the XOR distance:
  $$\text{bucket\_index} = \text{bit\_length}(x \oplus y) - 1$$
- **NodeContact**: Tracks a peer's `NodeID`, IP address, port, and last seen timestamp.

#### 2. K-Bucket Routing Table (`dht/routing.py`)
- **KBucket**: Stores up to $k=20$ peer contacts ordered by least-recently seen (head) to most-recently seen (tail). If a bucket is full, new contacts are placed in a replacement cache. If an active contact is removed, the freshest replacement contact is promoted automatically.
- **RoutingTable**: Manages 160 k-buckets. Provides an $O(\log N)$ closest-peer search (`find_closest_nodes`), returning up to $k$ contacts sorted by XOR distance.

#### 3. UDP Transport & Discovery Protocol (`dht/discovery.py`)
- Built on `asyncio.DatagramProtocol` for non-blocking UDP communication.
- Standard RPCs:
  - `PING` / `PONG`: Verify that a peer is online and measure round-trip latency.
  - `FIND_NODE` / `FOUND_NODES`: Ask a peer for the $k$ closest nodes it knows to a given target ID.
- Every valid packet received updates the sender's entry in our local routing table.

#### 4. Bootstrap & Iterative Lookup
- **Bootstrap Join**: A new node starts with only the IP:port of a bootstrap seed. It pings the seed to learn its NodeID, inserts it into its routing table, and then runs an iterative lookup for its own NodeID to find nearest neighbors.
- **$\alpha$-Concurrent Iterative Lookup**: Queries $\alpha=3$ closest unqueried peers concurrently, merges returned contacts into the shortlist, and repeats until convergence.

---

### Integration with Other Members

#### Member 1 (Integrator / `node.py`)
```python
from dht.discovery import DHTDiscoveryNode

dht = DHTDiscoveryNode(host="127.0.0.1", port=8001)
await dht.start()
# Join via bootstrap seed if not the seed node:
await dht.bootstrap([("127.0.0.1", 8001)])
```

#### Member 2 (P2P Networking Developer / TCP Sockets)
Member 2's TCP connection layer can query the DHT routing table to find live peer IP and port endpoints:
```python
peers = dht.get_known_peers()
for peer in peers:
    connect_to(peer.host, peer.port)
```

#### Member 4 (Task Engine Developer / `scheduler/router.py`)
Member 3 provides discovered candidate worker nodes to Member 4's task router:
```python
candidates = dht.export_worker_candidates()
for c in candidates:
    task_router.update_worker_metrics(
        node_id=c["node_id"],
        host=c["host"],
        port=c["port"],
        cpu_percent=0.0,
        ram_percent=0.0,
        active_tasks=0,
    )
```

#### Member 5 (Gossip & Dashboard / `dashboard/cli.py`)
Provides topology snapshots for terminal rendering:
```python
stats = dht.telemetry_snapshot()
# Returns total_contacts, pings_sent, pings_received, etc.
```

---

### Verification
- **Unit Tests**: `python -m unittest tests/test_dht.py` (13 tests)
- **10-Node Dynamic Mesh Audit**: `python examples/network_audit_demo.py`
- **Interactive Walkthrough**: `python examples/dht_discovery_demo.py`
