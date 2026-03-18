"""Tests for NodeRegistry geolocator integration."""

from unittest.mock import AsyncMock, patch

import pytest

from tracker_host.config import Config, ServerConfig
from tracker_host.node_registry import NodeRegistry


def _make_config(geolocator_enabled=True, port_base=31000):
    """Create a Config with geolocator settings."""
    return Config(
        output_dir="/tmp/test-geo-output",
        server=ServerConfig(
            host="0.0.0.0",
            port=8080,
            geolocator_enabled=geolocator_enabled,
            geolocator_tcp_port_base=port_base,
        ),
    )


class TestNodeRegistryGeolocator:
    @pytest.fixture
    def registry_no_geo(self, tmp_path):
        config = _make_config(geolocator_enabled=False)
        config.output_dir = str(tmp_path)
        return NodeRegistry(output_dir=str(tmp_path), global_config=config)

    @pytest.fixture
    def registry_with_geo(self, tmp_path):
        config = _make_config(geolocator_enabled=True)
        config.output_dir = str(tmp_path)
        return NodeRegistry(output_dir=str(tmp_path), global_config=config)

    @pytest.mark.asyncio
    async def test_no_geolocator_when_disabled(self, registry_no_geo):
        """No geolocator started when geolocator_enabled is False."""
        await registry_no_geo.register_node(
            "node1", {"location": {"rx": {}, "tx": {}}}
        )
        assert registry_no_geo._nodes["node1"].geolocator is None
        await registry_no_geo.close()

    @pytest.mark.asyncio
    async def test_no_geolocator_without_location(self, registry_with_geo):
        """No geolocator started when config lacks location data."""
        await registry_with_geo.register_node("node1", {"some": "config"})
        assert registry_with_geo._nodes["node1"].geolocator is None
        await registry_with_geo.close()

    @pytest.mark.asyncio
    @patch("tracker_host.node_registry.GeolocatorInstance")
    async def test_geolocator_started_with_location(
        self, MockGeo, registry_with_geo
    ):
        """Geolocator started when enabled and config has location."""
        mock_geo = AsyncMock()
        MockGeo.return_value = mock_geo
        mock_geo._radar_config_path = None

        config_data = {
            "location": {
                "rx": {"lat": 1, "lon": 2},
                "tx": {"lat": 3, "lon": 4},
            }
        }
        await registry_with_geo.register_node("node1", config_data)

        mock_geo.start.assert_awaited_once()
        assert registry_with_geo._nodes["node1"].geolocator is mock_geo
        await registry_with_geo.close()

    @pytest.mark.asyncio
    @patch("tracker_host.node_registry.GeolocatorInstance")
    async def test_track_event_forwarded_to_geolocator(
        self, MockGeo, registry_with_geo
    ):
        """Track events forwarded to geolocator when running."""
        mock_geo = AsyncMock()
        MockGeo.return_value = mock_geo
        mock_geo._radar_config_path = None

        config_data = {
            "location": {
                "rx": {"lat": 1, "lon": 2},
                "tx": {"lat": 3, "lon": 4},
            }
        }
        await registry_with_geo.register_node("node1", config_data)

        event = '{"track_id": "t1"}'
        await registry_with_geo.handle_track_event("node1", event)

        mock_geo.send_track_event.assert_awaited_once_with(event)
        await registry_with_geo.close()

    @pytest.mark.asyncio
    @patch("tracker_host.node_registry.GeolocatorInstance")
    async def test_geolocator_stopped_on_close(
        self, MockGeo, registry_with_geo
    ):
        """Geolocator stopped when registry is closed."""
        mock_geo = AsyncMock()
        MockGeo.return_value = mock_geo
        mock_geo._radar_config_path = None

        config_data = {"location": {"rx": {}, "tx": {}}}
        await registry_with_geo.register_node("node1", config_data)

        await registry_with_geo.close()
        mock_geo.stop.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("tracker_host.node_registry.GeolocatorInstance")
    async def test_geolocator_port_increments(
        self, MockGeo, registry_with_geo
    ):
        """Each node gets a unique geolocator port."""
        mock_geo = AsyncMock()
        MockGeo.return_value = mock_geo
        mock_geo._radar_config_path = None

        await registry_with_geo.register_node(
            "node1", {"location": {"rx": {}, "tx": {}}}
        )
        await registry_with_geo.register_node(
            "node2", {"location": {"rx": {}, "tx": {}}}
        )

        calls = MockGeo.call_args_list
        port1 = calls[0].kwargs["geo_config"].tcp_port
        port2 = calls[1].kwargs["geo_config"].tcp_port
        assert port1 == 31000
        assert port2 == 31001
        await registry_with_geo.close()

    @pytest.mark.asyncio
    @patch("tracker_host.node_registry.GeolocatorInstance")
    async def test_geolocator_start_failure_logged(
        self, MockGeo, registry_with_geo
    ):
        """Geolocator start failure is logged, not raised."""
        mock_geo = AsyncMock()
        MockGeo.return_value = mock_geo
        mock_geo._radar_config_path = None
        mock_geo.start.side_effect = RuntimeError("port in use")

        config_data = {"location": {"rx": {}, "tx": {}}}
        await registry_with_geo.register_node("node1", config_data)

        # Should not raise
        assert registry_with_geo._nodes["node1"].geolocator is None
        await registry_with_geo.close()

    @pytest.mark.asyncio
    async def test_no_geolocator_without_global_config(self, tmp_path):
        """No geolocator when no global_config provided."""
        registry = NodeRegistry(output_dir=str(tmp_path))
        await registry.register_node(
            "node1", {"location": {"rx": {}, "tx": {}}}
        )
        assert registry._nodes["node1"].geolocator is None
        await registry.close()
