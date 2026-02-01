"""Manager: orchestrates multiple TrackerInstance objects."""

import asyncio
import logging
import signal
from datetime import datetime
from typing import Optional

import aiohttp

from .config import Config, load_config
from .instance import TrackerInstance

logger = logging.getLogger(__name__)


class TrackerManager:
    """Main orchestrator that manages all tracker instances."""

    def __init__(self, config: Config):
        self.config = config
        self._instances: list[TrackerInstance] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._status_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the manager and all configured tracker instances."""
        if self._running:
            return

        self._running = True

        # Create shared session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",  # Avoid brotli issues
            },
        )

        # Create instances
        for tracker_config in self.config.trackers:
            instance = TrackerInstance(
                tracker_config=tracker_config,
                global_config=self.config,
                session=self._session,
            )
            self._instances.append(instance)

        logger.info(f"Starting {len(self._instances)} tracker instance(s)")

        # Start all instances concurrently
        results = await asyncio.gather(
            *(instance.start() for instance in self._instances),
            return_exceptions=True,
        )
        for instance, result in zip(self._instances, results):
            if isinstance(result, Exception):
                logger.error(
                    f"Failed to start instance {instance.name}: {result}"
                )

        # Start status reporting
        self._status_task = asyncio.create_task(
            self._status_loop(), name="status-loop"
        )

        logger.info("All tracker instances started")

    async def stop(self) -> None:
        """Stop the manager and all tracker instances."""
        if not self._running:
            return

        self._running = False

        logger.info("Stopping tracker manager...")

        # Stop status task
        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        # Stop all instances concurrently
        await asyncio.gather(
            *(instance.stop() for instance in self._instances),
            return_exceptions=True,
        )

        self._instances.clear()

        # Close shared session
        if self._session is not None:
            await self._session.close()
            self._session = None

        logger.info("Tracker manager stopped")

    async def _status_loop(self) -> None:
        """Periodically print status to console."""
        while self._running:
            await asyncio.sleep(self.config.status_interval_sec)

            if not self._running:
                break

            self._print_status()

    def _print_status(self) -> None:
        """Print current status to console."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [f"[{timestamp}] Status:"]
        for instance in self._instances:
            lines.append(f"  {instance.get_status()}")

        print("\n".join(lines))


async def run_manager(config_path: str, verbose: bool = False) -> None:
    """Run the tracker manager with the given config."""
    # Set up logging
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load config
    config = load_config(config_path)

    if not config.trackers:
        logger.error("No trackers configured")
        return

    # Create and start manager
    manager = TrackerManager(config)

    # Set up signal handlers
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await manager.start()

        # Wait for shutdown signal
        await stop_event.wait()

    finally:
        await manager.stop()
