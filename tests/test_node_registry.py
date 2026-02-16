"""Tests for NodeRegistry."""

import glob
import json
import pytest
from tracker_host.node_registry import NodeRegistry


@pytest.fixture
def registry(tmp_path):
    return NodeRegistry(output_dir=str(tmp_path))


class TestNodeRegistry:
    @pytest.mark.asyncio
    async def test_register_node(self, registry):
        config_data = {"location": {"rx": {"latitude": 33.9}}}
        await registry.register_node("radar3", config_data)
        nodes = registry.list_nodes()
        assert "radar3" in nodes
        assert nodes["radar3"]["has_config"] is True

    @pytest.mark.asyncio
    async def test_handle_track_event_writes_jsonl(self, registry, tmp_path):
        await registry.register_node("radar3", {"location": {}})
        event = {"track_id": "test-001", "length": 5, "timestamp": 1000}
        await registry.handle_track_event("radar3", json.dumps(event))

        # Check output file was created
        files = glob.glob(str(tmp_path / "radar3_*.jsonl"))
        assert len(files) == 1
        with open(files[0]) as f:
            line = f.readline()
            assert json.loads(line)["track_id"] == "test-001"

    @pytest.mark.asyncio
    async def test_handle_track_auto_registers(self, registry):
        event = {"track_id": "auto-001", "length": 1, "timestamp": 1000}
        await registry.handle_track_event("newnode", json.dumps(event))
        nodes = registry.list_nodes()
        assert "newnode" in nodes

    @pytest.mark.asyncio
    async def test_list_nodes_empty(self, registry):
        assert registry.list_nodes() == {}

    @pytest.mark.asyncio
    async def test_get_node_config(self, registry):
        config_data = {"capture": {"fc": 195000000}}
        await registry.register_node("radar3", config_data)
        assert registry.get_node_config("radar3") == config_data
        assert registry.get_node_config("missing") is None
