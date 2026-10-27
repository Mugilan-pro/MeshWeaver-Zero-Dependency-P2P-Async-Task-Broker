"""
network package
Part of MeshWeaver: Zero-Dependency P2P Async Task Broker
Role: Member 2 (P2P Networking Developer)

Exports the primary networking primitives:
- MessageProtocol: Length-prefixed framing and JSON serialization over TCP streams.
- P2PServer: Asynchronous server for listening and dispatching peer requests.
- P2PClient: Resilient client for communicating with remote peers with timeout protection.
"""

from network.protocol import MessageProtocol
from network.server import P2PServer
from network.client import P2PClient

__all__ = [
    "MessageProtocol",
    "P2PServer",
    "P2PClient",
]
