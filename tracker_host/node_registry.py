"""NodeRegistry: dynamic registration and management of radar nodes."""

import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

from .output_handler import OutputHandler

logger = logging.getLogger(__name__)

_VALID_NODE_NAME = re.compile(r"^[a-zA-Z0-9_-]+$")


@dataclass
class RegisteredNode:
    """A registered radar node with its config and output handler."""

    config: dict[str, Any]
    output_handler: OutputHandler


class NodeRegistry:
    """Manages dynamically registered radar nodes.

    Nodes register by POSTing their config. Each registered node gets
    an OutputHandler for writing track events to daily JSONL files.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self._nodes: dict[str, RegisteredNode] = {}

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

    async def handle_track_event(self, name: str, event_line: str) -> None:
        """Handle a track event from a node."""
        if name not in self._nodes:
            await self.register_node(name, {})

        node = self._nodes[name]
        await node.output_handler.handle_event(event_line)

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
        """Close all output handlers."""
        for node in self._nodes.values():
            await node.output_handler.close()
        self._nodes.clear()
