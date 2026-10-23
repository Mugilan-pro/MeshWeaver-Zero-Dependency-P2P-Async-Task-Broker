"""
example_node.py
Demonstration of MeshWeaver Core Networking Layer.
Shows how two P2P nodes (Server + Client) communicate asynchronously
without any external dependencies.

Usage:
    python example_node.py
"""

import asyncio
import logging
import sys
from typing import Any, Dict, Optional

from network import MessageProtocol, P2PServer, P2PClient

# Configure clean logging output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MeshWeaver.Demo")


async def node_message_router(message: Dict[str, Any], sender_addr: Any) -> Optional[Dict[str, Any]]:
    """
    Example message handler callback.
    Routes messages based on type (DHT, Gossip, Task Broker).
    """
    msg_type = message.get("type")
    sender_id = message.get("sender_id", "anonymous")
    logger.info("Handling '%s' message from %s (address: %s)", msg_type, sender_id, sender_addr)

    if msg_type == "PING":
        return {
            "type": "PONG",
            "reply_to": sender_id,
            "status": "OK",
            "timestamp": message.get("timestamp"),
        }

    elif msg_type == "DHT_FIND_NODE":
        target_id = message.get("target_id")
        return {
            "type": "DHT_NODES_FOUND",
            "target_id": target_id,
            "closest_peers": [
                {"peer_id": "node_beta", "host": "127.0.0.1", "port": 9002},
                {"peer_id": "node_gamma", "host": "127.0.0.1", "port": 9003},
            ],
        }

    elif msg_type == "TASK_EXECUTE":
        task_data = message.get("task", {})
        task_name = task_data.get("name")
        logger.info("Executing async task '%s'...", task_name)
        # Simulate async task work
        await asyncio.sleep(0.1)
        return {
            "type": "TASK_RESULT",
            "task_id": task_data.get("id"),
            "status": "SUCCESS",
            "output": f"Result of {task_name}: 42",
        }

    return {
        "type": "ERROR",
        "error": "UnsupportedMessageType",
        "detail": f"Unknown message type '{msg_type}'",
    }


async def main() -> None:
    logger.info("=== MeshWeaver Core Networking Layer Demo ===")

    # 1. Spin up Node Alpha (P2PServer) on localhost:9001
    server_alpha = P2PServer(
        host="127.0.0.1",
        port=9001,
        message_handler=node_message_router,
    )
    await server_alpha.start()
    logger.info("Node Alpha server is live on 127.0.0.1:9001\n")

    # 2. Spin up Node Beta (P2PClient) to connect to Node Alpha
    client_beta = P2PClient(host="127.0.0.1", port=9001, timeout=3.0)

    # --- Scenario A: PING / PONG ---
    logger.info("--- Scenario A: Sending PING message ---")
    ping_payload = {
        "type": "PING",
        "sender_id": "node_beta",
        "timestamp": 1729661200,
    }
    response = await client_beta.send_message(ping_payload)
    logger.info("Response received: %s\n", response)

    # --- Scenario B: DHT Routing Query ---
    logger.info("--- Scenario B: Sending DHT_FIND_NODE query ---")
    dht_payload = {
        "type": "DHT_FIND_NODE",
        "sender_id": "node_beta",
        "target_id": "0xDEADBEEF",
    }
    response = await client_beta.send_message(dht_payload)
    logger.info("Response received: %s\n", response)

    # --- Scenario C: Task Broker Execution Request ---
    logger.info("--- Scenario C: Submitting Async Task for Execution ---")
    task_payload = {
        "type": "TASK_EXECUTE",
        "sender_id": "node_beta",
        "task": {
            "id": "task_xyz_001",
            "name": "DistributedPrimeFactorization",
            "params": {"number": 104729},
        },
    }
    response = await client_beta.send_message(task_payload)
    logger.info("Response received: %s\n", response)

    # --- Scenario D: Error handling on offline peer (ConnectionRefusedError) ---
    logger.info("--- Scenario D: Attempting connection to offline peer (port 9999) ---")
    offline_client = P2PClient(host="127.0.0.1", port=9999, timeout=1.0)
    failed_response = await offline_client.send_message({"type": "PING"})
    logger.info("Graceful response from offline peer (expected None): %s\n", failed_response)

    # 3. Clean shutdown
    logger.info("Shutting down Node Alpha server...")
    await server_alpha.stop()
    logger.info("Demo completed successfully!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user.")
        sys.exit(0)
