"""Manager: orchestrates multiple TrackerInstance objects."""

import asyncio
import logging
import signal
from datetime import datetime
from typing import Any, Optional

import aiohttp

from .config import Config, TrackerConfig, load_config
from .fetcher import DetectionFetcher
from .instance import TrackerInstance
from .tcp_receiver import TcpReceiver
from .tcp_server import TcpServer

logger = logging.getLogger(__name__)


class TrackerManager:
    """Main orchestrator that manages all tracker instances."""

    def __init__(self, config: Config):
        self.config = config
        self._instances: list[TrackerInstance] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._status_task: Optional[asyncio.Task] = None
        self._tcp_server: Optional[TcpServer] = None
        self._used_ports: set[int] = set()

    async def start(self) -> None:
        """Start the manager and all configured tracker instances."""
        if self._running:
            return

        self._running = True

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
        )

        tcp_cfg = self.config.tcp_server
        if tcp_cfg.enabled:
            self._tcp_server = TcpServer(token=tcp_cfg.token)
            await self._tcp_server.start(tcp_cfg.host, tcp_cfg.port)
            if tcp_cfg.auto_provision:
                self._tcp_server.set_provision_callback(self._provision_node)

        for tracker_config in self.config.trackers:
            mode = tracker_config.mode or "http"

            if mode == "http":
                if not tracker_config.detection_url:
                    logger.error(
                        f"Tracker '{tracker_config.name}' is http mode but has no detection_url, skipping"
                    )
                    continue

                source = DetectionFetcher(
                    url=tracker_config.detection_url,
                    retry_config=self.config.retry,
                    session=self._session,
                )
                instance = TrackerInstance(
                    tracker_config=tracker_config,
                    global_config=self.config,
                    source=source,
                    session=self._session,
                )
                self._instances.append(instance)
                self._used_ports.add(tracker_config.tcp_port)

            elif mode == "tcp":
                if self._tcp_server is None:
                    logger.error(
                        f"Tracker '{tracker_config.name}' is tcp mode but tcp_server is not enabled, skipping"
                    )
                    continue

                node_id = tracker_config.node_id or tracker_config.name
                receiver = TcpReceiver(node_id)
                self._tcp_server.register_receiver(node_id, receiver)

                instance = TrackerInstance(
                    tracker_config=tracker_config,
                    global_config=self.config,
                    source=receiver,
                    session=self._session,
                )
                self._instances.append(instance)
                self._used_ports.add(tracker_config.tcp_port)

            else:
                logger.error(
                    f"Unknown mode '{mode}' for tracker '{tracker_config.name}', skipping"
                )

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

        self._status_task = asyncio.create_task(self._status_loop(), name="status-loop")

        logger.info("All tracker instances started")

    async def stop(self) -> None:
        """Stop the manager and all tracker instances."""
        if not self._running:
            return

        self._running = False

        logger.info("Stopping tracker manager...")

        if self._tcp_server is not None:
            await self._tcp_server.stop()
            self._tcp_server = None

        if self._status_task is not None:
            self._status_task.cancel()
            try:
                await self._status_task
            except asyncio.CancelledError:
                pass

        await asyncio.gather(
            *(instance.stop() for instance in self._instances),
            return_exceptions=True,
        )

        self._instances.clear()
        self._used_ports.clear()

        if self._session is not None:
            await self._session.close()
            self._session = None

        logger.info("Tracker manager stopped")

    def _provision_node(self, node_id: str, frame: dict[str, Any]) -> None:
        """Auto-provision a new tracker instance for an unknown node_id."""
        if node_id in {(tc.node_id or tc.name) for tc in self.config.trackers}:
            return

        for inst in self._instances:
            existing_node_id = inst.config.node_id or inst.config.name
            if existing_node_id == node_id:
                return

        tcp_cfg = self.config.tcp_server
        port = self._allocate_port(
            tcp_cfg.auto_provision_port_start, tcp_cfg.auto_provision_port_end
        )
        if port is None:
            logger.error(
                f"Cannot auto-provision node '{node_id}': port range exhausted"
            )
            return

        tracker_config = TrackerConfig(
            name=node_id,
            tcp_port=port,
            mode="tcp",
            node_id=node_id,
        )

        receiver = TcpReceiver(node_id)
        self._tcp_server.register_receiver(node_id, receiver)

        instance = TrackerInstance(
            tracker_config=tracker_config,
            global_config=self.config,
            source=receiver,
            session=self._session,
        )
        self._instances.append(instance)
        self._used_ports.add(port)

        asyncio.create_task(instance.start(), name=f"auto-provision-{node_id}")

        logger.info(f"Auto-provisioned tracker for node '{node_id}' on port {port}")

    def _allocate_port(self, start: int, end: int) -> Optional[int]:
        """Find the next available port in the given range."""
        for port in range(start, end + 1):
            if port not in self._used_ports:
                return port
        return None

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
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config(config_path)

    if not config.trackers and not config.tcp_server.enabled:
        logger.error("No trackers configured and TCP server not enabled")
        return

    manager = TrackerManager(config)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await manager.start()
        await stop_event.wait()
    finally:
        await manager.stop()
