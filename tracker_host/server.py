"""HTTP receive server for accepting track events from remote nodes."""

import json
import logging

from aiohttp import web

from .node_registry import NodeRegistry

logger = logging.getLogger(__name__)


def create_app(registry: NodeRegistry) -> web.Application:
    """Create the aiohttp web application."""
    app = web.Application()
    app["registry"] = registry

    app.router.add_post("/api/node/{name}/config", handle_config)
    app.router.add_post("/api/node/{name}/tracks", handle_tracks)
    app.router.add_get("/api/nodes", handle_list_nodes)

    return app


async def handle_config(request: web.Request) -> web.Response:
    """Receive and store radar config from a node."""
    name = request.match_info["name"]
    registry: NodeRegistry = request.app["registry"]

    try:
        config_data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "message": "invalid JSON"}, status=400
        )

    try:
        await registry.register_node(name, config_data)
    except ValueError as exc:
        return web.json_response(
            {"status": "error", "message": str(exc)}, status=400
        )

    logger.info(f"Received config from node: {name}")
    return web.json_response({"status": "registered", "node": name})


async def handle_tracks(request: web.Request) -> web.Response:
    """Receive track events from a node (JSONL: one event per line)."""
    name = request.match_info["name"]
    registry: NodeRegistry = request.app["registry"]

    body = await request.text()

    try:
        for line in body.strip().split("\n"):
            line = line.strip()
            if line:
                await registry.handle_track_event(name, line)
    except ValueError as exc:
        return web.json_response(
            {"status": "error", "message": str(exc)}, status=400
        )

    return web.json_response({"status": "ok"})


async def handle_list_nodes(request: web.Request) -> web.Response:
    """List all registered nodes (for debugging)."""
    registry: NodeRegistry = request.app["registry"]
    return web.json_response({"nodes": registry.list_nodes()})
