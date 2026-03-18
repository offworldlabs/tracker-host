"""Tests for the HTTP receive server."""

import json
import pytest
import pytest_asyncio

from tracker_host.server import create_app
from tracker_host.node_registry import NodeRegistry


@pytest.fixture
def registry(tmp_path):
    return NodeRegistry(output_dir=str(tmp_path))


@pytest.fixture
def app(registry):
    return create_app(registry)


@pytest_asyncio.fixture
async def client(aiohttp_client, app):
    return await aiohttp_client(app)


class TestReceiveServer:
    @pytest.mark.asyncio
    async def test_post_config(self, client):
        config = {"location": {"rx": {"latitude": 33.9}}}
        resp = await client.post(
            "/api/node/radar3/config",
            json=config,
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "registered"
        assert body["node"] == "radar3"

    @pytest.mark.asyncio
    async def test_post_track(self, client):
        event = {"track_id": "t-001", "length": 5, "timestamp": 1000}
        resp = await client.post(
            "/api/node/radar3/tracks",
            data=json.dumps(event),
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_list_nodes_empty(self, client):
        resp = await client.get("/api/nodes")
        assert resp.status == 200
        body = await resp.json()
        assert body["nodes"] == {}

    @pytest.mark.asyncio
    async def test_list_nodes_after_registration(self, client):
        await client.post("/api/node/radar3/config", json={"location": {}})
        resp = await client.get("/api/nodes")
        body = await resp.json()
        assert "radar3" in body["nodes"]

    @pytest.mark.asyncio
    async def test_post_track_batch(self, client):
        """Test posting multiple events as newline-delimited JSON."""
        events = [
            {"track_id": "t-001", "length": 3, "timestamp": 1000},
            {"track_id": "t-002", "length": 5, "timestamp": 1001},
        ]
        body = "\n".join(json.dumps(e) for e in events)
        resp = await client.post(
            "/api/node/radar3/tracks",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_post_config_invalid_name(self, client):
        """Test that an invalid node name returns 400."""
        resp = await client.post(
            "/api/node/bad%20name!/config",
            json={"location": {}},
        )
        assert resp.status == 400
        body = await resp.json()
        assert body["status"] == "error"

    @pytest.mark.asyncio
    async def test_post_track_invalid_name(self, client):
        """Test that an invalid node name returns 400 for track events."""
        event = {"track_id": "t-001", "length": 5, "timestamp": 1000}
        resp = await client.post(
            "/api/node/bad%20name!/tracks",
            data=json.dumps(event),
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert resp.status == 400
        body = await resp.json()
        assert body["status"] == "error"

    @pytest.mark.asyncio
    async def test_post_config_invalid_json(self, client):
        """Test that invalid JSON body returns 400."""
        resp = await client.post(
            "/api/node/radar3/config",
            data="not valid json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status == 400
        body = await resp.json()
        assert body["status"] == "error"
