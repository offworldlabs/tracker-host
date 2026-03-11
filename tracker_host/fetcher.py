"""HTTP detection fetcher with retry and backoff logic."""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp

from .config import RetryConfig

logger = logging.getLogger(__name__)


class ExtendedOutageError(Exception):
    """Raised when the detection endpoint has been unavailable for too long."""

    pass


@dataclass
class FetcherState:
    """Tracks fetcher health state."""

    consecutive_failures: int = 0
    first_failure_time: Optional[float] = None
    last_timestamp: Optional[int] = None
    is_healthy: bool = True


class DetectionFetcher:
    """Async HTTP fetcher for detection data with retry logic."""

    def __init__(
        self,
        url: str,
        retry_config: RetryConfig,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.url = url
        self.retry_config = retry_config
        self._session = session
        self._owns_session = session is None
        self.state = FetcherState()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate",  # Avoid brotli issues
                },
            )
        return self._session

    async def close(self) -> None:
        """Close the session if we own it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def is_healthy(self) -> bool:
        return self.state.is_healthy

    async def receive(self) -> Optional[dict[str, Any]]:
        """
        Fetch detection data from the endpoint.

        Returns the detection dict if successful and new data is available.
        Returns None if the data hasn't changed (same timestamp).
        Raises ExtendedOutageError if failures exceed the threshold.
        """
        session = await self._get_session()

        try:
            async with session.get(self.url) as response:
                response.raise_for_status()
                # Read raw text and parse JSON manually to avoid content-type issues
                text = await response.text()
                data = json.loads(text)

                # Reset failure tracking on success
                self._reset_failure_state()

                # Check if this is new data
                timestamp = data.get("timestamp")
                if timestamp is not None and timestamp == self.state.last_timestamp:
                    return None

                self.state.last_timestamp = timestamp
                return data

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            await self._handle_failure(e)
            return None

    async def _handle_failure(self, error: Exception) -> None:
        """Handle a fetch failure with exponential backoff."""
        now = time.time()

        if self.state.first_failure_time is None:
            self.state.first_failure_time = now

        self.state.consecutive_failures += 1
        self.state.is_healthy = False

        failure_duration = now - self.state.first_failure_time

        # Check for extended outage
        if failure_duration >= self.retry_config.extended_outage_sec:
            logger.error(
                f"Extended outage detected for {self.url} "
                f"(down for {failure_duration:.1f}s)"
            )
            raise ExtendedOutageError(
                f"Endpoint {self.url} unavailable for {failure_duration:.1f}s"
            )

        # Calculate backoff delay
        backoff = min(
            self.retry_config.backoff_base_sec**self.state.consecutive_failures,
            30.0,  # Cap at 30 seconds
        )

        logger.warning(
            f"Fetch failed for {self.url}: {error}. "
            f"Retry {self.state.consecutive_failures}, backoff {backoff:.1f}s"
        )

        await asyncio.sleep(backoff)

    def _reset_failure_state(self) -> None:
        """Reset failure tracking after successful fetch."""
        if not self.state.is_healthy:
            logger.info(f"Connection restored for {self.url}")

        self.state.consecutive_failures = 0
        self.state.first_failure_time = None
        self.state.is_healthy = True

    async def wait_for_recovery(self) -> bool:
        """
        Wait and periodically check if the endpoint is back.

        Returns True when the endpoint is reachable again.
        """
        session = await self._get_session()

        logger.info(f"Waiting for {self.url} to recover...")

        while True:
            await asyncio.sleep(self.retry_config.health_check_interval_sec)

            try:
                async with session.get(self.url) as response:
                    response.raise_for_status()
                    self._reset_failure_state()
                    logger.info(f"Endpoint {self.url} is back online")
                    return True
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.debug(f"Recovery check failed for {self.url}: {e}")
                continue
