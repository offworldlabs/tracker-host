"""Server mode: receive track events from remote nodes via HTTP."""

import asyncio
import logging
import signal

from aiohttp import web

from .config import load_config
from .node_registry import NodeRegistry
from .server import create_app

logger = logging.getLogger(__name__)


async def run_server(config_path: str, verbose: bool = False) -> None:
    """Run tracker-host in server mode."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config(config_path)
    registry = NodeRegistry(output_dir=config.output_dir, global_config=config)
    app = create_app(registry)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, config.server.host, config.server.port)
    await site.start()

    logger.info(
        f"Server listening on http://{config.server.host}:{config.server.port}"
    )

    # Wait for shutdown signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await stop_event.wait()
    finally:
        await registry.close()
        await runner.cleanup()
        logger.info("Server stopped")
