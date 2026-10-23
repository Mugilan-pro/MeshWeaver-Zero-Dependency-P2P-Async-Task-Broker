"""
network/client.py
Part of MeshWeaver: Zero-Dependency P2P Async Task Broker
Role: Member 2 (P2P Networking Developer)

Implements the asynchronous TCP client for peer-to-peer communication.
Opens connections to target peers, transmits framed JSON messages, waits for
responses, and gracefully handles network partitions, timeouts, and connection refusals.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from network.protocol import MessageProtocol

logger = logging.getLogger("MeshWeaver.Network.Client")


class P2PClient:
    """
    Asynchronous Peer-to-Peer TCP Client.

    Used by nodes to communicate with remote peers in the mesh.
    All socket operations are strictly protected by timeout boundaries using
    asyncio.wait_for and fail gracefully without crashing the host process.
    """

    def __init__(
        self,
        host: str,
        port: int,
        timeout: float = 5.0,
    ) -> None:
        """
        Initializes the P2P Client with target peer coordinates.

        Args:
            host: Target peer hostname or IP address.
            port: Target peer port.
            timeout: Default timeout in seconds for connection and I/O operations.
        """
        self.host: str = host
        self.port: int = port
        self.timeout: float = timeout

    async def send_message(
        self,
        message_dict: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Opens a connection to the target peer, transmits a framed message,
        waits for the response, and ensures the connection is closed.

        Strictly wrapped in asyncio.wait_for to prevent blocking indefinitely.
        Catches ConnectionRefusedError, TimeoutError, and other network failures
        gracefully and logs warnings instead of raising unhandled exceptions.

        Args:
            message_dict: The dictionary payload to send.
            timeout: Optional timeout override in seconds. Defaults to self.timeout.

        Returns:
            Optional[Dict[str, Any]]: Parsed response dictionary from the peer,
                                      or None if an error or timeout occurred.
        """
        effective_timeout = self.timeout if timeout is None else timeout

        try:
            return await asyncio.wait_for(
                self._send_and_receive(message_dict),
                timeout=effective_timeout,
            )
        except ConnectionRefusedError as exc:
            logger.warning(
                "Connection refused: peer at %s:%d is offline or not accepting connections (%s).",
                self.host,
                self.port,
                exc,
            )
            return None
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning(
                "Operation timed out after %.2f seconds while communicating with %s:%d.",
                effective_timeout,
                self.host,
                self.port,
            )
            return None
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
            logger.warning(
                "Connection dropped by peer %s:%d during transaction: %s",
                self.host,
                self.port,
                exc,
            )
            return None
        except OSError as exc:
            logger.warning(
                "Socket OS error communicating with peer %s:%d: %s",
                self.host,
                self.port,
                exc,
            )
            return None
        except Exception as exc:
            logger.error(
                "Unexpected error communicating with peer %s:%d: %s",
                self.host,
                self.port,
                exc,
                exc_info=True,
            )
            return None

    async def _send_and_receive(
        self,
        message_dict: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Internal coroutine executing the connect -> frame -> send -> receive -> close flow.
        """
        logger.debug("Connecting to peer at %s:%d...", self.host, self.port)
        reader, writer = await asyncio.open_connection(self.host, self.port)

        try:
            # Step 1: Encode and frame the message
            framed_data = await MessageProtocol.encode_message(message_dict)

            # Step 2: Transmit bytes and flush buffer
            writer.write(framed_data)
            await writer.drain()
            logger.debug("Transmitted framed message (%d bytes) to %s:%d", len(framed_data), self.host, self.port)

            # Step 3: Wait for peer response
            response = await MessageProtocol.read_message(reader)
            logger.debug("Received response from %s:%d: %s", self.host, self.port, response)
            return response

        finally:
            # Step 4: Always cleanly close socket
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError):
                pass
            except Exception as exc:
                logger.debug("Error while closing client socket to %s:%d: %s", self.host, self.port, exc)

    @classmethod
    async def send_to(
        cls,
        host: str,
        port: int,
        message_dict: Dict[str, Any],
        timeout: float = 5.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Convenience class method to send a one-shot message to any peer
        without manually managing client instances.

        Ideal for DHT lookups, heartbeat pings, and gossip broadcast rounds.

        Args:
            host: Peer host/IP.
            port: Peer port.
            message_dict: Payload dictionary to send.
            timeout: Timeout in seconds.

        Returns:
            Optional[Dict[str, Any]]: Response from peer or None.
        """
        client = cls(host=host, port=port, timeout=timeout)
        return await client.send_message(message_dict)
