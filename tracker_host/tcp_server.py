"""Asyncio TCP server for receiving push-based detection frames from radar nodes."""

import asyncio
import json
import logging
import socket
from typing import Any, Callable, Optional

from .tcp_receiver import TcpReceiver

logger = logging.getLogger(__name__)

MAX_LINE_LENGTH = 1_048_576


class TcpServer:
    """Single TCP server that routes detection frames to registered TcpReceivers."""

    def __init__(self, token: str):
        self._token = token
        self._receivers: dict[str, TcpReceiver] = {}
        self._provision_callback: Optional[Callable[[str, dict[str, Any]], None]] = None
        self._server: Optional[asyncio.AbstractServer] = None
        self._connections: dict[asyncio.Task, asyncio.StreamWriter] = {}
        self._connection_node_ids: dict[asyncio.Task, str] = {}

    def register_receiver(self, node_id: str, receiver: TcpReceiver) -> None:
        self._receivers[node_id] = receiver

    def set_provision_callback(
        self, callback: Callable[[str, dict[str, Any]], None]
    ) -> None:
        self._provision_callback = callback

    async def start(self, host: str, port: int) -> None:
        self._server = await asyncio.start_server(self._handle_connection, host, port)
        logger.info(f"TCP server listening on {host}:{port}")

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for task, writer in list(self._connections.items()):
            if not writer.is_closing():
                writer.close()
            task.cancel()

        self._connections.clear()
        self._connection_node_ids.clear()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        peername = writer.get_extra_info("peername")
        task = asyncio.current_task()
        self._connections[task] = writer

        buffer = ""
        authenticated = False
        node_id: Optional[str] = None

        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break

                buffer += data.decode("utf-8", errors="replace")

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)

                    if len(line) > MAX_LINE_LENGTH:
                        logger.warning(
                            f"Message exceeds max length from {peername}, dropping"
                        )
                        continue

                    if not line.strip():
                        continue

                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON from {peername}: {line[:100]}")
                        continue

                    if not authenticated:
                        msg_token = msg.get("token")
                        if msg_token != self._token:
                            logger.warning(
                                f"Invalid token from {peername}, closing connection"
                            )
                            transport = writer.transport
                            if transport is not None:
                                transport.abort()
                            return
                        authenticated = True

                    msg_node_id = msg.get("node_id")
                    if msg_node_id is None:
                        logger.warning(f"Message without node_id from {peername}")
                        continue

                    if node_id is None:
                        node_id = msg_node_id
                        self._connection_node_ids[task] = node_id
                        if node_id in self._receivers:
                            self._receivers[node_id].set_connected(True)

                    if msg.get("type") == "heartbeat":
                        continue

                    if msg_node_id in self._receivers:
                        self._receivers[msg_node_id].put(msg)
                    elif self._provision_callback is not None:
                        self._provision_callback(msg_node_id, msg)
                        if msg_node_id in self._receivers:
                            self._receivers[msg_node_id].set_connected(True)
                            self._receivers[msg_node_id].put(msg)

                if len(buffer) > MAX_LINE_LENGTH:
                    logger.warning(
                        f"Buffer exceeds max length from {peername}, clearing"
                    )
                    buffer = ""

        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception:
            logger.exception(f"Error handling connection from {peername}")
        finally:
            if node_id is not None and node_id in self._receivers:
                self._receivers[node_id].set_connected(False)

            self._connections.pop(task, None)
            self._connection_node_ids.pop(task, None)

            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
