"""
tests/test_network.py
Unit and integration tests for MeshWeaver Network Layer.
Zero external dependencies - runs with standard library unittest and asyncio.

Usage:
    python -m unittest tests/test_network.py
    python tests/test_network.py
"""

import asyncio
import struct
import unittest
from typing import Any, Dict, Optional

from network.protocol import MessageProtocol
from network.server import P2PServer
from network.client import P2PClient


class TestMessageProtocol(unittest.IsolatedAsyncioTestCase):
    """Verifies length-prefixed framing and JSON encoding/decoding."""

    async def test_encode_and_read_roundtrip(self):
        """Test encoding a dict and reading it back from a StreamReader."""
        original_data = {
            "type": "TASK_SUBMIT",
            "task_id": "task_1001",
            "payload": {"numbers": [1, 2, 3, 4], "action": "sum"},
            "nested": {"author": "MeshWeaver Member 2", "status": "PENDING"},
        }

        # Encode
        framed_bytes = await MessageProtocol.encode_message(original_data)

        # Header check: exactly 4 bytes
        self.assertGreater(len(framed_bytes), 4)
        length_header = framed_bytes[:4]
        (payload_len,) = struct.unpack("!I", length_header)
        self.assertEqual(payload_len, len(framed_bytes) - 4)

        # Feed to StreamReader and read back
        reader = asyncio.StreamReader()
        reader.feed_data(framed_bytes)
        reader.feed_eof()

        decoded_data = await MessageProtocol.read_message(reader)
        self.assertEqual(decoded_data, original_data)

    async def test_tcp_fragmentation_handling(self):
        """
        Simulates TCP packet fragmentation where bytes arrive in tiny 2-byte chunks.
        Verifies MessageProtocol reassembles the frame accurately.
        """
        sample_dict = {"status": "HEALTHY", "nodes_count": 42}
        framed_bytes = await MessageProtocol.encode_message(sample_dict)

        reader = asyncio.StreamReader()

        # Feed in fragmented 2-byte chunks with yield points
        async def feed_slowly():
            chunk_size = 2
            for i in range(0, len(framed_bytes), chunk_size):
                reader.feed_data(framed_bytes[i : i + chunk_size])
                await asyncio.sleep(0.001)
            reader.feed_eof()

        feeder_task = asyncio.create_task(feed_slowly())
        decoded = await MessageProtocol.read_message(reader)
        await feeder_task

        self.assertEqual(decoded, sample_dict)

    async def test_eof_handling(self):
        """Clean EOF should return None."""
        reader = asyncio.StreamReader()
        reader.feed_eof()

        result = await MessageProtocol.read_message(reader)
        self.assertIsNone(result)

    async def test_incomplete_header_handling(self):
        """Incomplete header followed by EOF should return None without error."""
        reader = asyncio.StreamReader()
        reader.feed_data(b"\x00\x00")  # Only 2 of 4 bytes
        reader.feed_eof()

        result = await MessageProtocol.read_message(reader)
        self.assertIsNone(result)


class TestP2PNetworking(unittest.IsolatedAsyncioTestCase):
    """Verifies end-to-end P2PServer and P2PClient interaction over real TCP loopback."""

    async def asyncSetUp(self):
        # Sample message handler for the server
        async def handle_message(message: Dict[str, Any], peer_addr) -> Optional[Dict[str, Any]]:
            msg_type = message.get("type")
            if msg_type == "PING":
                return {"type": "PONG", "echo": message.get("payload")}
            elif msg_type == "TASK_COMPUTE":
                nums = message.get("numbers", [])
                return {"type": "TASK_RESULT", "sum": sum(nums)}
            elif msg_type == "SLOW_OPERATION":
                await asyncio.sleep(message.get("delay", 2.0))
                return {"type": "SLOW_DONE"}
            return {"type": "UNKNOWN"}

        # Bind to port 0 for dynamic ephemeral port allocation (prevents port conflicts)
        self.server = P2PServer(host="127.0.0.1", port=0, message_handler=handle_message)
        await self.server.start()
        self.port = self.server.actual_port
        self.assertIsNotNone(self.port)
        self.assertGreater(self.port, 0)

    async def asyncTearDown(self):
        await self.server.stop()

    async def test_client_server_ping_pong(self):
        """Test basic ping-pong exchange."""
        client = P2PClient(host="127.0.0.1", port=self.port, timeout=3.0)
        req = {"type": "PING", "payload": "hello-mesh"}
        res = await client.send_message(req)

        self.assertIsNotNone(res)
        self.assertEqual(res.get("type"), "PONG")
        self.assertEqual(res.get("echo"), "hello-mesh")

    async def test_client_server_task_computation(self):
        """Test task computation request and result."""
        client = P2PClient(host="127.0.0.1", port=self.port, timeout=3.0)
        req = {"type": "TASK_COMPUTE", "numbers": [10, 20, 30, 40]}
        res = await client.send_message(req)

        self.assertIsNotNone(res)
        self.assertEqual(res.get("type"), "TASK_RESULT")
        self.assertEqual(res.get("sum"), 100)

    async def test_convenience_send_to(self):
        """Test P2PClient.send_to class method."""
        req = {"type": "PING", "payload": "one-shot"}
        res = await P2PClient.send_to("127.0.0.1", self.port, req, timeout=3.0)

        self.assertIsNotNone(res)
        self.assertEqual(res.get("type"), "PONG")
        self.assertEqual(res.get("echo"), "one-shot")

    async def test_timeout_handling(self):
        """Test that slow response triggers timeout and returns None without crashing."""
        client = P2PClient(host="127.0.0.1", port=self.port, timeout=0.2)
        req = {"type": "SLOW_OPERATION", "delay": 1.0}
        res = await client.send_message(req)

        # Must return None without throwing unhandled exceptions
        self.assertIsNone(res)

    async def test_connection_refused_handling(self):
        """Test connecting to a non-existent port returns None without crashing."""
        # Use an unused high port (e.g., 59999)
        client = P2PClient(host="127.0.0.1", port=59999, timeout=1.0)
        req = {"type": "PING"}
        res = await client.send_message(req)

        # Must return None without crashing
        self.assertIsNone(res)


if __name__ == "__main__":
    unittest.main()
