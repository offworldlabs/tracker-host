"""GeolocatorInstance: manages one retina-geolocator subprocess per radar."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import aiohttp
import yaml

from .config import Config, GeolocatorConfig
from .output_handler import OutputHandler
from .utils import check_port_available, connect_tcp, start_subprocess, stop_subprocess

logger = logging.getLogger(__name__)


class GeolocatorInstance:
    """Manages one retina-geolocator subprocess for a radar."""

    def __init__(
        self,
        name: str,
        geo_config: GeolocatorConfig,
        global_config: Config,
        config_url: Optional[str],
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.name = name
        self.geo_config = geo_config
        self.global_config = global_config
        self.config_url = config_url
        self._session = session

        self.output_handler = OutputHandler(
            name=f"{name}_solutions",
            output_dir=global_config.output_dir,
        )

        self._process: Optional[asyncio.subprocess.Process] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._output_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._running = False
        self._radar_config_path: Optional[str] = None

    async def start(self) -> None:
        """Fetch radar config, spawn geolocator subprocess, connect TCP."""
        if self._running:
            return

        self._running = True

        # Fetch radar config from the radar node and save to temp file
        await self._fetch_radar_config()

        # Start geolocator subprocess
        await self._start_process()

        # Connect TCP to send track events
        await self._connect_tcp()

        # Start reading solutions from stdout
        self._output_task = asyncio.create_task(
            self._output_loop(), name=f"{self.name}-geo-output"
        )

        # Drain stderr to logger so the pipe buffer never fills
        self._stderr_task = asyncio.create_task(
            self._stderr_loop(), name=f"{self.name}-geo-stderr"
        )

        logger.info(f"Geolocator started for {self.name}")

    async def stop(self) -> None:
        """Stop the geolocator subprocess and clean up."""
        if not self._running:
            return

        self._running = False

        # Cancel output and stderr loops
        for task_attr in ("_output_task", "_stderr_task"):
            task = getattr(self, task_attr)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, task_attr, None)

        # Close TCP connection
        await self._close_tcp()

        # Stop subprocess
        await self._stop_process()

        # Close output handler
        await self.output_handler.close()

        logger.info(f"Geolocator stopped for {self.name}")

    async def send_track_event(self, line: str) -> None:
        """Send a track event line to the geolocator via TCP."""
        if self._tcp_writer is None:
            return

        try:
            data = line.strip() + "\n"
            self._tcp_writer.write(data.encode())
            await self._tcp_writer.drain()
        except (ConnectionError, OSError) as e:
            logger.error(f"Geolocator TCP send error for {self.name}: {e}")

    async def _fetch_radar_config(self) -> None:
        """Fetch the radar's config from its config_url and save to a temp file."""
        if not self.config_url:
            raise RuntimeError(
                f"No config_url for {self.name}, required for geolocator"
            )

        if self._session is None:
            raise RuntimeError(f"No HTTP session available for {self.name}")

        logger.info(f"Fetching radar config for {self.name} from {self.config_url}")

        async with self._session.get(self.config_url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        # Save to output directory as a temp YAML file
        output_dir = Path(self.global_config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        config_path = output_dir / f".{self.name}_config.yml"

        with open(config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False)

        self._radar_config_path = str(config_path.resolve())
        logger.info(f"Saved radar config to {config_path}")

    async def _start_process(self) -> None:
        """Start the retina-geolocator subprocess."""
        if not check_port_available(self.geo_config.tcp_port):
            raise RuntimeError(
                f"Port {self.geo_config.tcp_port} is already in use for "
                f"geolocator '{self.name}'"
            )

        geo_path = Path(self.global_config.retina_geolocator_path)

        cmd = [
            sys.executable,
            "-m",
            "scripts.stream_solutions",
            "--tcp",
            "--tcp-port",
            str(self.geo_config.tcp_port),
            "--radar-config",
            self._radar_config_path,
        ]

        if self.geo_config.geolocator_config_path:
            cmd.extend(["--config", self.geo_config.geolocator_config_path])

        self._process = await start_subprocess(
            cmd,
            cwd=geo_path,
            startup_delay=self.global_config.geolocator_startup_delay_sec,
            label=f"geolocator for {self.name}",
        )

    async def _stop_process(self) -> None:
        """Stop the geolocator subprocess."""
        await stop_subprocess(self._process, label=f"geolocator for {self.name}")
        self._process = None

    async def _connect_tcp(self) -> None:
        """Connect to the geolocator's TCP port."""
        self._tcp_reader, self._tcp_writer = await connect_tcp(
            "127.0.0.1",
            self.geo_config.tcp_port,
            max_retries=self.global_config.tcp_connect_retries,
            retry_delay=self.global_config.tcp_retry_delay_sec,
            label=f"geolocator for {self.name}",
        )

    async def _close_tcp(self) -> None:
        """Close the TCP connection to the geolocator."""
        if self._tcp_writer is not None:
            self._tcp_writer.close()
            try:
                await self._tcp_writer.wait_closed()
            except Exception:
                pass
            self._tcp_writer = None
            self._tcp_reader = None

    async def _output_loop(self) -> None:
        """Read geolocator stdout and pass solutions to the output handler."""
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
                logger.error(f"Error reading geolocator output for {self.name}: {e}")
                await asyncio.sleep(0.1)

    async def _stderr_loop(self) -> None:
        """Drain geolocator stderr to logger so the pipe buffer never fills."""
        while self._running:
            if self._process is None or self._process.stderr is None:
                await asyncio.sleep(0.1)
                continue

            try:
                line = await asyncio.wait_for(
                    self._process.stderr.readline(), timeout=1.0
                )

                if line:
                    logger.info(f"[{self.name}] {line.decode().strip()}")

            except asyncio.TimeoutError:
                continue

            except asyncio.CancelledError:
                raise

            except Exception:
                await asyncio.sleep(0.1)

    def get_status(self) -> str:
        """Get status string for this geolocator."""
        return self.output_handler.get_status()
