"""Output handling: file writing and optional API forwarding."""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import aiohttp

from .config import ApiForwardConfig

logger = logging.getLogger(__name__)


@dataclass
class TrackMetrics:
    """Metrics for tracking output statistics."""

    active_tracks: dict[str, int] = field(default_factory=dict)  # track_id -> length

    @property
    def count(self) -> int:
        """Number of active tracks."""
        return len(self.active_tracks)

    @property
    def lengths(self) -> list[int]:
        """List of track lengths."""
        return list(self.active_tracks.values())

    def update_from_event(self, event: dict[str, Any]) -> None:
        """Update metrics from a track event."""
        track_id = event.get("track_id")
        if not track_id:
            return

        # Get track length from event
        length = event.get("length") or event.get("n_associated") or 0
        self.active_tracks[track_id] = length

    def clear(self) -> None:
        """Clear all metrics."""
        self.active_tracks.clear()


class OutputHandler:
    """Handles tracker output: writes to files and optionally forwards to API."""

    def __init__(
        self,
        name: str,
        output_dir: str | Path,
        api_config: Optional[ApiForwardConfig] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.name = name
        self.output_dir = Path(output_dir)
        self.api_config = api_config
        self._session = session
        self._owns_session = session is None
        self.metrics = TrackMetrics()

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Current output file handle
        self._current_file: Optional[Any] = None
        self._current_date: Optional[str] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self) -> None:
        """Close resources."""
        if self._current_file is not None:
            self._current_file.close()
            self._current_file = None

        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _get_output_file(self) -> Any:
        """Get the current output file, rotating daily."""
        today = datetime.now().strftime("%Y-%m-%d")

        if self._current_date != today:
            # Close old file
            if self._current_file is not None:
                self._current_file.close()

            # Open new file
            filename = f"{self.name}_{today}.jsonl"
            filepath = self.output_dir / filename
            self._current_file = open(filepath, "a")
            self._current_date = today
            logger.info(f"Opened output file: {filepath}")

        return self._current_file

    async def handle_event(self, event_line: str) -> None:
        """
        Process a single tracker output event.

        Writes to file and optionally forwards to API.
        """
        event_line = event_line.strip()
        if not event_line:
            return

        # Parse for metrics
        try:
            event = json.loads(event_line)
            self.metrics.update_from_event(event)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON event: {event_line[:100]}")
            return

        # Write to file
        self._write_to_file(event_line)

        # Forward to API if configured
        if self.api_config and self.api_config.enabled:
            await self._forward_to_api(event_line)

    def _write_to_file(self, event_line: str) -> None:
        """Write event to the output file."""
        f = self._get_output_file()
        f.write(event_line + "\n")
        f.flush()

    async def _forward_to_api(self, event_line: str) -> None:
        """Forward event to the configured API endpoint."""
        if not self.api_config or not self.api_config.url:
            return

        session = await self._get_session()

        headers = {"Content-Type": "application/json"}
        headers.update(self.api_config.headers)

        try:
            async with session.post(
                self.api_config.url,
                data=event_line,
                headers=headers,
            ) as response:
                if response.status >= 400:
                    logger.warning(
                        f"API forward failed: {response.status} for {self.name}"
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"API forward error for {self.name}: {e}")

    def get_status(self) -> str:
        """Get a status string for this handler."""
        count = self.metrics.count
        lengths = self.metrics.lengths

        if count == 0:
            return f"{self.name}: no active tracks"

        lengths_str = ", ".join(
            str(length) for length in sorted(lengths, reverse=True)[:10]
        )
        if count > 10:
            lengths_str += f", ... ({count - 10} more)"

        return f"{self.name}: {count} active tracks (lengths: {lengths_str})"
