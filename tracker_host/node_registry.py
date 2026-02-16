"""NodeRegistry: dynamic registration and management of radar nodes."""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

from .config import Config, GeolocatorConfig
from .geolocator import GeolocatorInstance
from .output_handler import OutputHandler

logger = logging.getLogger(__name__)

_VALID_NODE_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass
class RegisteredNode:
    """A registered radar node with its config and output handler."""

    config: dict[str, Any]
    output_handler: OutputHandler
    geolocator: Optional[GeolocatorInstance] = None


class NodeRegistry:
    """Manages dynamically registered radar nodes.

    Nodes register by POSTing their config. Each registered node gets
    an OutputHandler for writing track events to daily JSONL files.
    When geolocator is enabled, nodes with location data also get a
    GeolocatorInstance for converting tracks to geographic solutions.
    """

    def __init__(self, output_dir: str, global_config: Optional[Config] = None):
        self.output_dir = output_dir
        self.global_config = global_config
        self._nodes: dict[str, RegisteredNode] = {}
        self._next_geo_port: int = (
            global_config.server.geolocator_tcp_port_base
            if global_config and global_config.server
            else 31000
        )

    @property
    def _geolocator_enabled(self) -> bool:
        return (
            self.global_config is not None
            and self.global_config.server.geolocator_enabled
        )

    async def register_node(self, name: str, config_data: dict[str, Any]) -> None:
        """Register a node or update its config."""
        if not _VALID_NODE_NAME.match(name):
            raise ValueError(
                f"Invalid node name {name!r}: must match [a-zA-Z0-9_-]+"
            )
        if name not in self._nodes:
            output_handler = OutputHandler(
                name=name,
                output_dir=self.output_dir,
            )
            self._nodes[name] = RegisteredNode(
                config=config_data,
                output_handler=output_handler,
            )
            logger.info(f"Registered new node: {name}")
        else:
            self._nodes[name].config = config_data
            logger.info(f"Updated config for node: {name}")

        # Start geolocator if enabled and config has location data
        if (
            self._geolocator_enabled
            and config_data.get("location")
            and self._nodes[name].geolocator is None
        ):
            await self._start_geolocator(name, config_data)

    async def _start_geolocator(self, name: str, config_data: dict) -> None:
        """Start a geolocator instance for a node."""
        port = self._next_geo_port
        self._next_geo_port += 1

        # Save radar config to file so geolocator can read it
        output_path = Path(self.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        config_path = output_path / f".{name}_radar_config.yml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False)

        geo_config = GeolocatorConfig(enabled=True, tcp_port=port)
        geo = GeolocatorInstance(
            name=name,
            geo_config=geo_config,
            global_config=self.global_config,
            config_url=None,
            session=None,
        )
        # Provide the saved config path so _fetch_radar_config is skipped
        geo._radar_config_path = str(config_path.resolve())

        try:
            await geo.start()
            self._nodes[name].geolocator = geo
            logger.info(f"Started geolocator for {name} on port {port}")
        except Exception as e:
            logger.error(f"Failed to start geolocator for {name}: {e}")

    async def handle_track_event(self, name: str, event_line: str) -> None:
        """Handle a track event from a node."""
        if name not in self._nodes:
            await self.register_node(name, {})

        node = self._nodes[name]
        await node.output_handler.handle_event(event_line)

        # Forward to geolocator if running
        if node.geolocator is not None:
            await node.geolocator.send_track_event(event_line)

    def get_node_config(self, name: str) -> Optional[dict]:
        """Get stored config for a node, or None if not registered."""
        if name in self._nodes:
            return self._nodes[name].config
        return None

    def list_nodes(self) -> dict[str, Any]:
        """List all registered nodes with summary info."""
        return {
            name: {
                "has_config": bool(node.config),
                "active_tracks": node.output_handler.metrics.count,
            }
            for name, node in self._nodes.items()
        }

    async def close(self) -> None:
        """Close all output handlers and stop geolocators."""
        for node in self._nodes.values():
            if node.geolocator is not None:
                await node.geolocator.stop()
            await node.output_handler.close()
        self._nodes.clear()
