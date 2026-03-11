"""TrackerInstance: manages one detection source and one retina-tracker subprocess."""

import asyncio
import json
import logging
import sys
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import aiohttp

from .config import Config, TrackerConfig
from .detection_source import DetectionSource
from .fetcher import DetectionFetcher, ExtendedOutageError
from .geolocator import GeolocatorInstance
from .output_handler import OutputHandler
from .utils import check_port_available, connect_tcp, start_subprocess, stop_subprocess

logger = logging.getLogger(__name__)


class InstanceState(Enum):
    """State of a tracker instance."""

    STARTING = auto()
    RUNNING = auto()
    RECONNECTING = auto()
    STOPPED = auto()


class TrackerInstance:
    """Manages one detection endpoint + one retina-tracker subprocess."""

    def __init__(
        self,
        tracker_config: TrackerConfig,
        global_config: Config,
        source: DetectionSource,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.config = tracker_config
        self.global_config = global_config
        self.name = tracker_config.name
        self.state = InstanceState.STOPPED
        self._session = session

        # Determine API forward config (per-instance overrides global)
        api_config = tracker_config.api_forward or global_config.api_forward

        # Components
        self.source = source
        self.output_handler = OutputHandler(
            name=self.name,
            output_dir=global_config.output_dir,
            api_config=api_config,
            session=session,
        )

        # Geolocator (optional)
        self.geolocator: Optional[GeolocatorInstance] = None
        if tracker_config.geolocator is not None and tracker_config.geolocator.enabled:
            self.geolocator = GeolocatorInstance(
                name=self.name,
                geo_config=tracker_config.geolocator,
                global_config=global_config,
                config_url=tracker_config.config_url,
                session=session,
            )

        # Subprocess
        self._process: Optional[asyncio.subprocess.Process] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._tcp_reader: Optional[asyncio.StreamReader] = None

        # Control
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Start the tracker instance."""
        if self._running:
            return

        self._running = True
        self.state = InstanceState.STARTING

        logger.info(f"Starting tracker instance: {self.name}")

        # Start the retina-tracker subprocess
        await self._start_tracker_process()

        # Connect to tracker via TCP
        await self._connect_to_tracker()

        # Start geolocator if configured
        if self.geolocator is not None:
            await self.geolocator.start()

        self.state = InstanceState.RUNNING

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._fetch_loop(), name=f"{self.name}-fetch"),
            asyncio.create_task(self._output_loop(), name=f"{self.name}-output"),
        ]

        if self.config.config_url:
            self._tasks.append(
                asyncio.create_task(
                    self._config_poll_loop(), name=f"{self.name}-config-poll"
                )
            )

    async def stop(self) -> None:
        """Stop the tracker instance."""
        if not self._running:
            return

        self._running = False
        self.state = InstanceState.STOPPED

        logger.info(f"Stopping tracker instance: {self.name}")

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._tasks.clear()

        # Stop geolocator first
        if self.geolocator is not None:
            await self.geolocator.stop()

        # Close TCP connection
        await self._close_tcp()

        # Stop subprocess
        await self._stop_tracker_process()

        # Close handlers
        await self.source.close()
        await self.output_handler.close()

    async def _start_tracker_process(self) -> None:
        """Start the retina-tracker subprocess."""
        if not check_port_available(self.config.tcp_port):
            raise RuntimeError(
                f"Port {self.config.tcp_port} is already in use. "
                f"Choose a different tcp_port for tracker '{self.name}'"
            )

        tracker_path = Path(self.global_config.retina_tracker_path)

        cmd = [
            sys.executable,
            "-m",
            "tracker.track_detections",
            "--tcp",
            "--tcp-host",
            "127.0.0.1",
            "--tcp-port",
            str(self.config.tcp_port),
            "-s",
            "-",  # Stream output to stdout
        ]

        if self.config.tracker_config:
            cmd.extend(["-c", self.config.tracker_config])

        self._process = await start_subprocess(
            cmd,
            cwd=tracker_path,
            startup_delay=self.global_config.startup_delay_sec,
            label=f"retina-tracker for {self.name}",
        )

    async def _stop_tracker_process(self) -> None:
        """Stop the retina-tracker subprocess."""
        await stop_subprocess(self._process, label=f"tracker for {self.name}")
        self._process = None

    async def _connect_to_tracker(self) -> None:
        """Connect to the tracker's TCP port."""
        self._tcp_reader, self._tcp_writer = await connect_tcp(
            "127.0.0.1",
            self.config.tcp_port,
            max_retries=self.global_config.tcp_connect_retries,
            retry_delay=self.global_config.tcp_retry_delay_sec,
            label=f"tracker for {self.name}",
        )

    async def _close_tcp(self) -> None:
        """Close the TCP connection."""
        if self._tcp_writer is not None:
            self._tcp_writer.close()
            try:
                await self._tcp_writer.wait_closed()
            except Exception:
                pass
            self._tcp_writer = None
            self._tcp_reader = None

    async def _fetch_loop(self) -> None:
        """Main loop: fetch detections and send to tracker."""
        while self._running:
            try:
                data = await self.source.receive()

                if data is not None:
                    await self._send_to_tracker(data)

            except ExtendedOutageError:
                logger.error(f"Extended outage for {self.name}, stopping tracker")
                self.state = InstanceState.RECONNECTING

                # Stop geolocator
                if self.geolocator is not None:
                    await self.geolocator.stop()

                # Stop the tracker subprocess
                await self._close_tcp()
                await self._stop_tracker_process()

                # Clear metrics since tracker is down
                self.output_handler.metrics.clear()

                # Wait for recovery (only DetectionFetcher raises ExtendedOutageError)
                if isinstance(self.source, DetectionFetcher):
                    await self.source.wait_for_recovery()

                # Restart
                logger.info(f"Restarting tracker for {self.name}")
                await self._start_tracker_process()
                await self._connect_to_tracker()

                if self.geolocator is not None:
                    await self.geolocator.start()

                self.state = InstanceState.RUNNING

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.error(f"Unexpected error in fetch loop for {self.name}: {e}")
                await asyncio.sleep(1.0)

            await asyncio.sleep(self.global_config.poll_interval_sec)

    async def _send_to_tracker(self, data: dict) -> None:
        """Send detection data to the tracker via TCP."""
        if self._tcp_writer is None:
            logger.warning(f"No TCP connection for {self.name}, dropping data")
            return

        try:
            line = json.dumps(data) + "\n"
            self._tcp_writer.write(line.encode())
            await self._tcp_writer.drain()
        except (ConnectionError, OSError) as e:
            logger.error(f"TCP send error for {self.name}: {e}")

    async def _output_loop(self) -> None:
        """Read tracker output and process it."""
        while self._running:
            if self._process is None or self._process.stdout is None:
                await asyncio.sleep(0.1)
                continue

            try:
                line = await asyncio.wait_for(
                    self._process.stdout.readline(),
                    timeout=1.0,
                )

                if line:
                    decoded_line = line.decode()
                    await self.output_handler.handle_event(decoded_line)

                    # Forward track event to geolocator
                    if self.geolocator is not None:
                        await self.geolocator.send_track_event(decoded_line)

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.error(f"Error reading tracker output for {self.name}: {e}")
                await asyncio.sleep(0.1)

    async def _config_poll_loop(self) -> None:
        """Poll the radar node's config endpoint and restart on changes."""
        baseline: Optional[str] = None

        while self._running:
            await asyncio.sleep(self.global_config.config_poll_interval_sec)

            try:
                async with self._session.get(self.config.config_url) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                snapshot = json.dumps(data, sort_keys=True)

                if baseline is None:
                    baseline = snapshot
                    logger.info(f"Config baseline captured for {self.name}")
                elif snapshot != baseline:
                    logger.warning(
                        f"Config change detected for {self.name}, restarting tracker"
                    )
                    baseline = snapshot

                    self.state = InstanceState.RECONNECTING

                    if self.geolocator is not None:
                        await self.geolocator.stop()

                    await self._close_tcp()
                    await self._stop_tracker_process()
                    self.output_handler.metrics.clear()
                    await self._start_tracker_process()
                    await self._connect_to_tracker()

                    if self.geolocator is not None:
                        await self.geolocator.start()

                    self.state = InstanceState.RUNNING

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.warning(f"Config poll failed for {self.name}: {e}")

    def get_status(self) -> str:
        """Get status string for this instance."""
        state_str = self.state.name.lower()
        output_status = self.output_handler.get_status()
        status = f"[{state_str}] {output_status}"

        if self.geolocator is not None:
            geo_status = self.geolocator.get_status()
            status += f" | geo: {geo_status}"

        return status
