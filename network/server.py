"""
network/server.py
Part of MeshWeaver: Zero-Dependency P2P Async Task Broker
Role: Member 2 (P2P Networking Developer)

Implements the asynchronous TCP server for peer-to-peer communication.
Uses asyncio.start_server and MessageProtocol to serve peers concurrently,
dispatching incoming messages to a customizable handler and returning responses.
"""

import asyncio
import inspect
import logging
from typing import Any, Callable, Coroutine, Dict, Optional, Set, Tuple, Union

from network.protocol import MessageProtocol

logger = logging.getLogger("MeshWeaver.Network.Server")

# Type alias for message handler callbacks
# Can be sync or async, accepting either (message) or (message, peer_address)
MessageHandler = Callable[..., Union[Optional[Dict[str, Any]], Coroutine[Any, Any, Optional[Dict[str, Any]]]]]


class P2PServer:
    """
    Asynchronous Peer-to-Peer TCP Server.

    Listens for incoming peer connections, dispatches framed JSON messages
    to the configured message handler, and transmits back responses.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        message_handler: Optional[MessageHandler] = None,
    ) -> None:
        """
        Initializes the P2P Server.

        Args:
            host: Interface address to bind to (e.g. '0.0.0.0' or '127.0.0.1').
            port: Port number to listen on.
            message_handler: Async or sync callable to process parsed message dicts.
                             May return a dict response (to send back) or None.
        """
        self.host: str = host
        self.port: int = port
        self.message_handler: Optional[MessageHandler] = message_handler

        self._server: Optional[asyncio.Server] = None
        self._is_running: bool = False
        self._active_connections: Set[asyncio.StreamWriter] = set()

    @property
    def is_running(self) -> bool:
        """Returns True if the server is actively listening for connections."""
        return self._is_running and self._server is not None and self._server.is_serving()

    @property
    def actual_port(self) -> Optional[int]:
        """
        Returns the actual port bound to by the socket.
        Useful when initialized with port=0 (OS-assigned ephemeral port).
        """
        if self._server and self._server.sockets:
            return self._server.sockets[0].getsockname()[1]
        return self.port

    async def start(self) -> None:
        """
        Starts the asynchronous TCP server and begins accepting connections.
        Does not block; returns as soon as the server is listening.
        """
        if self.is_running:
            logger.warning("P2PServer is already running on %s:%d.", self.host, self.actual_port)
            return

        self._server = await asyncio.start_server(
            self.handle_client,
            host=self.host,
            port=self.port,
        )
        self._is_running = True

        bound_port = self.actual_port
        logger.info("P2PServer started and listening on %s:%d", self.host, bound_port)

    async def serve_forever(self) -> None:
        """
        Keeps the server running indefinitely until cancelled or stopped.
        """
        if not self._server:
            await self.start()

        if self._server:
            async with self._server:
                await self._server.serve_forever()

    async def stop(self) -> None:
        """
        Gracefully stops the server and closes all active peer connections.
        """
        if not self._is_running:
            return

        logger.info("Stopping P2PServer on %s:%d...", self.host, self.actual_port)
        self._is_running = False

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # Cleanly close all lingering client sockets
        close_tasks = []
        for writer in list(self._active_connections):
            try:
                writer.close()
                close_tasks.append(writer.wait_closed())
            except Exception as exc:
                logger.debug("Error while closing client socket during shutdown: %s", exc)

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._active_connections.clear()
        logger.info("P2PServer stopped successfully.")

    async def handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Client connection handler invoked by asyncio.start_server for each new peer.

        Continuously reads framed messages via MessageProtocol.read_message until
        the peer closes the connection or an error occurs.
        Dispatches messages to the message_handler and transmits responses.

        Args:
            reader: Stream reader for incoming peer bytes.
            writer: Stream writer for outgoing peer bytes.
        """
        peer_address: Tuple[str, int] = writer.get_extra_info("peername") or ("unknown", 0)
        logger.debug("Accepted connection from peer %s:%d", peer_address[0], peer_address[1])

        self._active_connections.add(writer)

        try:
            while self._is_running:
                # Read next complete message from stream
                message = await MessageProtocol.read_message(reader)
                if message is None:
                    # Clean EOF or socket dropped mid-frame; terminate session
                    logger.debug(
                        "Peer %s:%d disconnected or sent empty stream. Closing session.",
                        peer_address[0],
                        peer_address[1],
                    )
                    break

                logger.debug("Received message from %s:%d: %s", peer_address[0], peer_address[1], message)

                # Process message through handler callback
                response: Optional[Dict[str, Any]] = None
                if self.message_handler:
                    try:
                        response = await self._dispatch_handler(message, peer_address)
                    except Exception as handler_exc:
                        logger.error(
                            "Unhandled exception in message_handler for %s:%d: %s",
                            peer_address[0],
                            peer_address[1],
                            handler_exc,
                            exc_info=True,
                        )
                        response = {
                            "status": "error",
                            "error": "InternalHandlerError",
                            "message": str(handler_exc),
                        }

                # Send response back if one was produced
                if response is not None:
                    encoded_response = await MessageProtocol.encode_message(response)
                    writer.write(encoded_response)
                    await writer.drain()
                    logger.debug(
                        "Sent response to %s:%d: %s",
                        peer_address[0],
                        peer_address[1],
                        response,
                    )

        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as exc:
            logger.info("Peer %s:%d dropped connection: %s", peer_address[0], peer_address[1], exc)
        except asyncio.CancelledError:
            logger.debug("Client handler task cancelled for peer %s:%d", peer_address[0], peer_address[1])
            raise
        except Exception as exc:
            logger.error(
                "Unexpected error while serving peer %s:%d: %s",
                peer_address[0],
                peer_address[1],
                exc,
                exc_info=True,
            )
        finally:
            self._active_connections.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError):
                pass
            except Exception as exc:
                logger.debug("Error finalizing writer for %s:%d: %s", peer_address[0], peer_address[1], exc)

            logger.debug("Connection closed for peer %s:%d", peer_address[0], peer_address[1])

    async def _dispatch_handler(
        self,
        message: Dict[str, Any],
        peer_address: Tuple[str, int],
    ) -> Optional[Dict[str, Any]]:
        """
        Invokes the message_handler callback with adaptive signature support.
        Supports:
        - handler(message)
        - handler(message, peer_address)
        - Both async coroutines and regular synchronous functions.
        """
        if not self.message_handler:
            return None

        # Inspect parameters to support both 1-arg and 2-arg callbacks
        try:
            sig = inspect.signature(self.message_handler)
            param_count = len(sig.parameters)
        except (ValueError, TypeError):
            param_count = 1

        if param_count >= 2:
            result = self.message_handler(message, peer_address)
        else:
            result = self.message_handler(message)

        if inspect.isawaitable(result):
            return await result
        return result
