"""TrackerInstance: manages one detection source and one retina-tracker subprocess."""

import asyncio
import json
import logging
import socket
import sys
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import aiohttp

from .config import Config, TrackerConfig
from .detection_source import DetectionSource
from .fetcher import DetectionFetcher, ExtendedOutageError
from .output_handler import OutputHandler

logger = logging.getLogger(__name__)


def check_port_available(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a port is available for binding."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            return True
    except OSError:
        return False


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

        self.state = InstanceState.RUNNING

        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._fetch_loop(), name=f"{self.name}-fetch"),
            asyncio.create_task(self._output_loop(), name=f"{self.name}-output"),
        ]

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

        # Close TCP connection
        await self._close_tcp()

        # Stop subprocess
        await self._stop_tracker_process()

        # Close handlers
        await self.source.close()
        await self.output_handler.close()

    async def _start_tracker_process(self) -> None:
        """Start the retina-tracker subprocess."""
        # Check port availability before starting
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

        # Add custom tracker config if specified
        if self.config.tracker_config:
            cmd.extend(["-c", self.config.tracker_config])

        logger.info(f"Starting retina-tracker for {self.name}: {' '.join(cmd)}")

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=tracker_path,
        )

        # Give it a moment to start up
        await asyncio.sleep(1.0)

        if self._process.returncode is not None:
            stderr = await self._process.stderr.read()
            raise RuntimeError(f"Tracker process exited immediately: {stderr.decode()}")

        logger.info(
            f"Tracker process started for {self.name} (PID: {self._process.pid})"
        )

    async def _stop_tracker_process(self) -> None:
        """Stop the retina-tracker subprocess."""
        if self._process is None:
            return

        if self._process.returncode is None:
            logger.info(f"Terminating tracker process for {self.name}")
            self._process.terminate()

            try:
                await asyncio.wait_for(self._process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Force killing tracker process for {self.name}")
                self._process.kill()
                await self._process.wait()

        self._process = None

    async def _connect_to_tracker(self) -> None:
        """Connect to the tracker's TCP port."""
        max_retries = 10
        retry_delay = 0.5

        for attempt in range(max_retries):
            try:
                self._tcp_reader, self._tcp_writer = await asyncio.open_connection(
                    "127.0.0.1", self.config.tcp_port
                )
                logger.info(f"Connected to tracker TCP port {self.config.tcp_port}")
                return
            except (ConnectionRefusedError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.debug(
                        f"TCP connection attempt {attempt + 1} failed: {e}, retrying..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Failed to connect to tracker after {max_retries} attempts"
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
                    await self.output_handler.handle_event(line.decode())

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.error(f"Error reading tracker output for {self.name}: {e}")
                await asyncio.sleep(0.1)

    def get_status(self) -> str:
        """Get status string for this instance."""
        state_str = self.state.name.lower()
        output_status = self.output_handler.get_status()
        return f"[{state_str}] {output_status}"
