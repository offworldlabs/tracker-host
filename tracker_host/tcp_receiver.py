"""TCP push-based detection source backed by an asyncio queue."""

import asyncio
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 2000
RECEIVE_TIMEOUT = 0.5


class TcpReceiver:
    """Detection source that receives frames pushed by TcpServer."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=MAX_QUEUE_SIZE
        )
        self._last_timestamp: Optional[int] = None
        self._connected: bool = False

    async def receive(self) -> Optional[dict[str, Any]]:
        try:
            frame = await asyncio.wait_for(self._queue.get(), timeout=RECEIVE_TIMEOUT)
        except (asyncio.TimeoutError, TimeoutError):
            return None

        timestamp = frame.get("timestamp")
        if timestamp is not None and timestamp == self._last_timestamp:
            return None

        self._last_timestamp = timestamp
        return frame

    async def close(self) -> None:
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    @property
    def is_healthy(self) -> bool:
        return self._connected

    def set_connected(self, connected: bool) -> None:
        self._connected = connected

    def put(self, frame: dict[str, Any]) -> None:
        if self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            logger.warning(f"Queue full for node {self.node_id}, dropped oldest frame")
        self._queue.put_nowait(frame)
