"""
Unit tests for MeshWeaver DHT & Peer Discovery (Member 3).
Tests NodeID XOR distance, routing table k-buckets, and asyncio UDP discovery.
"""

import asyncio
import unittest

from dht.node_id import (
    DEFAULT_ID_BITS,
    NodeContact,
    NodeID,
    shared_prefix_length,
    xor_distance,
)
from dht.routing import KBucket, RoutingTable
from dht.discovery import DHTDiscoveryNode


class TestNodeIDAndDistance(unittest.TestCase):
    def test_node_id_generation(self):
        node_id = NodeID.generate()
        self.assertEqual(node_id.num_bits, 160)
        self.assertEqual(len(node_id.bytes_value), 20)
        self.assertEqual(len(node_id.hex_value), 40)
        self.assertGreaterEqual(node_id.int_value, 0)
        self.assertLess(node_id.int_value, 1 << 160)

    def test_node_id_from_int_and_hex(self):
        id_int = NodeID.from_int(12345)
        self.assertEqual(id_int.int_value, 12345)

        hex_str = "000000000000000000000000000000000000abcd"
        id_hex = NodeID.from_hex(hex_str)
        self.assertEqual(id_hex.hex_value, hex_str)
        self.assertEqual(id_hex.int_value, 0xabcd)

    def test_deterministic_endpoint_hashing(self):
        id1 = NodeID.from_endpoint("127.0.0.1", 8000)
        id2 = NodeID.from_endpoint("127.0.0.1", 8000)
        id3 = NodeID.from_endpoint("127.0.0.1", 8001)
        self.assertEqual(id1, id2)
        self.assertNotEqual(id1, id3)

    def test_xor_distance_math(self):
        a = NodeID.generate()
        b = NodeID.generate()
        c = NodeID.generate()

        # Identity
        self.assertEqual(a.xor_distance(a), 0)
        # Symmetry
        self.assertEqual(a.xor_distance(b), b.xor_distance(a))
        # Positivity
        self.assertGreater(a.xor_distance(b), 0)
        # Triangle inequality
        self.assertLessEqual(a.xor_distance(c), a.xor_distance(b) + b.xor_distance(c))

    def test_prefix_and_bucket_index(self):
        base = 0x1234567890ABCDEF1234567890ABCDEF12345670
        id1 = NodeID.from_int(base | 0x1)  # ends in 0001
        id2 = NodeID.from_int(base | 0x2)  # ends in 0010

        dist = id1.xor_distance(id2)
        self.assertEqual(dist, 3)
        self.assertEqual(id1.bucket_index(id2), 1)
        self.assertEqual(id1.shared_prefix_length(id2), 158)

        # Self bucket index is undefined
        with self.assertRaises(ValueError):
            id1.bucket_index(id1)

    def test_node_contact_dict_roundtrip(self):
        node_id = NodeID.generate()
        contact = NodeContact(node_id=node_id, host="127.0.0.1", port=9000, rtt_ms=12.5)

        data = contact.to_dict()
        restored = NodeContact.from_dict(data)
        self.assertEqual(contact.node_id, restored.node_id)
        self.assertEqual(contact.host, restored.host)
        self.assertEqual(contact.port, restored.port)
        self.assertEqual(contact.rtt_ms, restored.rtt_ms)


class TestRoutingTableAndKBuckets(unittest.TestCase):
    def test_kbucket_lru_behavior(self):
        bucket = KBucket(k=3)
        c1 = NodeContact(NodeID.generate(), port=9001)
        c2 = NodeContact(NodeID.generate(), port=9002)
        c3 = NodeContact(NodeID.generate(), port=9003)

        bucket.add_contact(c1)
        bucket.add_contact(c2)
        bucket.add_contact(c3)
        self.assertTrue(bucket.is_full)
        self.assertEqual(bucket.size, 3)

        # c1 is oldest, c3 is newest
        self.assertEqual(bucket.contacts[0].node_id, c1.node_id)
        self.assertEqual(bucket.contacts[2].node_id, c3.node_id)

        # Touching c1 should push it to newest (tail)
        bucket.add_contact(c1)
        self.assertEqual(bucket.contacts[0].node_id, c2.node_id)
        self.assertEqual(bucket.contacts[2].node_id, c1.node_id)

    def test_kbucket_replacement_cache_promotion(self):
        bucket = KBucket(k=2)
        c1 = NodeContact(NodeID.generate(), port=9001)
        c2 = NodeContact(NodeID.generate(), port=9002)
        c3 = NodeContact(NodeID.generate(), port=9003)

        bucket.add_contact(c1)
        bucket.add_contact(c2)

        # Adding 3rd node to full bucket buffers into replacement cache
        success, oldest = bucket.add_contact(c3)
        self.assertFalse(success)
        self.assertEqual(oldest.node_id, c1.node_id)
        self.assertEqual(len(bucket.replacement_cache), 1)

        # When c1 is removed, c3 is promoted into the active bucket
        bucket.remove_contact(c1.node_id)
        self.assertEqual(bucket.size, 2)
        self.assertEqual(bucket.contacts[1].node_id, c3.node_id)

    def test_routing_table_closest_nodes(self):
        local_id = NodeID.from_int(0)
        table = RoutingTable(local_node_id=local_id, k=5)

        for i in range(1, 11):
            table.add_or_update(NodeContact(node_id=NodeID.from_int(i * 100), port=8000 + i))

        self.assertEqual(table.total_contacts, 10)

        target = NodeID.from_int(250)
        closest = table.find_closest_nodes(target_id=target, count=3)
        self.assertEqual(len(closest), 3)

        # Must be strictly ordered by XOR distance to target
        d0 = closest[0].node_id.xor_distance(target)
        d1 = closest[1].node_id.xor_distance(target)
        d2 = closest[2].node_id.xor_distance(target)
        self.assertLessEqual(d0, d1)
        self.assertLessEqual(d1, d2)

    def test_bucket_refresh_id_generator(self):
        local_id = NodeID.generate()
        table = RoutingTable(local_node_id=local_id)

        target = table.generate_random_id_for_bucket(42)
        self.assertEqual(local_id.bucket_index(target), 42)


class TestDHTProtocolAndDiscovery(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def run_async(self, coro):
        return self.loop.run_until_complete(coro)

    def test_udp_ping_pong_week1(self):
        async def _test():
            node1 = DHTDiscoveryNode(host="127.0.0.1", port=0)
            node2 = DHTDiscoveryNode(host="127.0.0.1", port=0)
            await node1.start()
            await node2.start()

            try:
                success, rtt, peer_id = await node1.ping(node2.endpoint)
                self.assertTrue(success)
                self.assertGreater(rtt, 0.0)
                self.assertEqual(peer_id, node2.node_id)

                # Both should now have each other in routing tables
                self.assertEqual(node1.routing_table.total_contacts, 1)
                self.assertEqual(node2.routing_table.total_contacts, 1)
            finally:
                await node1.stop()
                await node2.stop()

        self.run_async(_test())

    def test_udp_find_node_week2(self):
        async def _test():
            node_a = DHTDiscoveryNode(host="127.0.0.1", port=0)
            node_b = DHTDiscoveryNode(host="127.0.0.1", port=0)
            node_c = DHTDiscoveryNode(host="127.0.0.1", port=0)
            await node_a.start()
            await node_b.start()
            await node_c.start()

            try:
                # Node B knows Node C
                node_b.routing_table.add_or_update(node_c.self_contact)

                # Node A pings Node B, then queries for Node C's ID
                await node_a.ping(node_b.endpoint)
                contact_b = node_a.routing_table.get_contact(node_b.node_id)
                self.assertIsNotNone(contact_b)

                discovered = await node_a.find_node(contact_b, node_c.node_id)
                self.assertTrue(any(c.node_id == node_c.node_id for c in discovered))
                self.assertIsNotNone(node_a.routing_table.get_contact(node_c.node_id))
            finally:
                await node_a.stop()
                await node_b.stop()
                await node_c.stop()

        self.run_async(_test())

    def test_bootstrap_mesh_joining(self):
        async def _test():
            nodes = [DHTDiscoveryNode(host="127.0.0.1", port=0) for _ in range(5)]
            for n in nodes:
                await n.start()

            try:
                bootstrap_addr = nodes[0].endpoint
                for n in nodes[1:]:
                    joined = await n.bootstrap([bootstrap_addr])
                    self.assertTrue(joined)

                for n in nodes[1:]:
                    self.assertGreaterEqual(n.routing_table.total_contacts, 1)
            finally:
                for n in nodes:
                    await n.stop()

        self.run_async(_test())

    def test_ping_unreachable_node_timeout(self):
        async def _test():
            node = DHTDiscoveryNode(host="127.0.0.1", port=0, rpc_timeout=0.2)
            await node.start()
            try:
                # Port 59999 has no listener
                success, rtt, peer_id = await node.ping(("127.0.0.1", 59999))
                self.assertFalse(success)
                self.assertIsNone(peer_id)
            finally:
                await node.stop()

        self.run_async(_test())


if __name__ == "__main__":
    unittest.main()
