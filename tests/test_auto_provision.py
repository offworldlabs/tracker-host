"""Tests for TrackerManager auto-provisioning and TcpServer integration."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from tracker_host.config import Config, RetryConfig, TcpServerConfig, TrackerConfig
from tracker_host.fetcher import DetectionFetcher
from tracker_host.manager import TrackerManager
from tracker_host.tcp_receiver import TcpReceiver

TOKEN = "test-secret"


def _make_config(
    trackers=None,
    tcp_enabled=True,
    auto_provision=True,
    port_start=31000,
    port_end=31099,
    tcp_port=0,
):
    return Config(
        output_dir="./output",
        poll_interval_sec=0.5,
        status_interval_sec=30.0,
        retry=RetryConfig(),
        tcp_server=TcpServerConfig(
            enabled=tcp_enabled,
            host="127.0.0.1",
            port=tcp_port,
            token=TOKEN,
            auto_provision=auto_provision,
            auto_provision_port_start=port_start,
            auto_provision_port_end=port_end,
        ),
        trackers=trackers or [],
    )


async def _get_free_port():
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port


async def _connect(port):
    return await asyncio.open_connection("127.0.0.1", port)


async def _send_and_drain(writer, msg):
    writer.write((json.dumps(msg) + "\n").encode())
    await writer.drain()


@pytest.fixture
def instance_patcher():
    with patch("tracker_host.manager.TrackerInstance") as mock_cls:

        def make_instance(**kwargs):
            inst = MagicMock()
            inst.start = AsyncMock()
            inst.stop = AsyncMock()
            inst.get_status = MagicMock(return_value="[running] ok")
            inst.config = kwargs.get(
                "tracker_config", TrackerConfig(name="mock", tcp_port=99999)
            )
            return inst

        mock_cls.side_effect = lambda **kwargs: make_instance(**kwargs)
        yield mock_cls


@pytest_asyncio.fixture
async def manager_with_tcp(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port)
    manager = TrackerManager(config)
    await manager.start()
    yield manager, tcp_port, instance_patcher
    await manager.stop()


@pytest.mark.asyncio
async def test_auto_provision_creates_instance(manager_with_tcp):
    manager, tcp_port, mock_cls = manager_with_tcp
    assert len(manager._instances) == 0

    _, writer = await _connect(tcp_port)
    frame = {"node_id": "new-node", "token": TOKEN, "timestamp": 100, "delay": [1.0]}
    await _send_and_drain(writer, frame)
    await asyncio.sleep(0.1)

    assert len(manager._instances) == 1
    call_kwargs = mock_cls.call_args
    assert call_kwargs[1]["tracker_config"].name == "new-node"
    assert call_kwargs[1]["tracker_config"].mode == "tcp"
    assert isinstance(call_kwargs[1]["source"], TcpReceiver)

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_auto_provision_receiver_registered(manager_with_tcp):
    manager, tcp_port, _ = manager_with_tcp

    _, writer = await _connect(tcp_port)
    frame = {"node_id": "node-a", "token": TOKEN, "timestamp": 200, "delay": [2.0]}
    await _send_and_drain(writer, frame)
    await asyncio.sleep(0.1)

    assert "node-a" in manager._tcp_server._receivers
    receiver = manager._tcp_server._receivers["node-a"]
    result = await receiver.receive()
    assert result is not None
    assert result["timestamp"] == 200

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_auto_provision_port_range(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port, port_start=32000, port_end=32002)
    manager = TrackerManager(config)
    await manager.start()

    try:
        for i, name in enumerate(["a", "b", "c"]):
            _, writer = await _connect(tcp_port)
            await _send_and_drain(
                writer,
                {"node_id": name, "token": TOKEN, "timestamp": i, "delay": [1.0]},
            )
            await asyncio.sleep(0.1)
            writer.close()
            await writer.wait_closed()

        assert len(manager._instances) == 3
        ports = {inst.config.tcp_port for inst in manager._instances}
        assert ports == {32000, 32001, 32002}
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_auto_provision_port_exhaustion(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port, port_start=33000, port_end=33000)
    manager = TrackerManager(config)
    await manager.start()

    try:
        _, w1 = await _connect(tcp_port)
        await _send_and_drain(
            w1, {"node_id": "first", "token": TOKEN, "timestamp": 1, "delay": [1.0]}
        )
        await asyncio.sleep(0.1)
        assert len(manager._instances) == 1

        _, w2 = await _connect(tcp_port)
        await _send_and_drain(
            w2, {"node_id": "second", "token": TOKEN, "timestamp": 2, "delay": [1.0]}
        )
        await asyncio.sleep(0.1)
        assert len(manager._instances) == 1

        w1.close()
        w2.close()
        await w1.wait_closed()
        await w2.wait_closed()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_reconnecting_node_reattaches(manager_with_tcp):
    manager, tcp_port, mock_cls = manager_with_tcp

    _, w1 = await _connect(tcp_port)
    await _send_and_drain(
        w1, {"node_id": "recon-node", "token": TOKEN, "timestamp": 1, "delay": [1.0]}
    )
    await asyncio.sleep(0.1)
    assert len(manager._instances) == 1
    creation_count = mock_cls.call_count

    receiver = manager._tcp_server._receivers["recon-node"]
    r1 = await receiver.receive()
    assert r1 is not None
    assert r1["timestamp"] == 1

    w1.close()
    await w1.wait_closed()
    await asyncio.sleep(0.1)

    _, w2 = await _connect(tcp_port)
    await _send_and_drain(
        w2, {"node_id": "recon-node", "token": TOKEN, "timestamp": 2, "delay": [2.0]}
    )
    await asyncio.sleep(0.1)

    assert len(manager._instances) == 1
    assert mock_cls.call_count == creation_count

    r2 = await receiver.receive()
    assert r2 is not None
    assert r2["timestamp"] == 2

    w2.close()
    await w2.wait_closed()


@pytest.mark.asyncio
async def test_tcp_mode_tracker_gets_receiver(instance_patcher):
    tcp_port = await _get_free_port()
    trackers = [
        TrackerConfig(name="tcp-node", tcp_port=30099, mode="tcp", node_id="tcp-node"),
    ]
    config = _make_config(trackers=trackers, tcp_port=tcp_port)
    manager = TrackerManager(config)
    await manager.start()

    try:
        assert len(manager._instances) == 1
        call_kwargs = instance_patcher.call_args
        assert isinstance(call_kwargs[1]["source"], TcpReceiver)
        assert "tcp-node" in manager._tcp_server._receivers
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_http_mode_tracker_gets_fetcher(instance_patcher):
    tcp_port = await _get_free_port()
    trackers = [
        TrackerConfig(
            name="http-node",
            tcp_port=30098,
            mode="http",
            detection_url="http://example.com/api/detection",
        ),
    ]
    config = _make_config(trackers=trackers, tcp_port=tcp_port)
    manager = TrackerManager(config)
    await manager.start()

    try:
        assert len(manager._instances) == 1
        call_kwargs = instance_patcher.call_args
        assert isinstance(call_kwargs[1]["source"], DetectionFetcher)
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_tcp_server_started_when_enabled(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port, tcp_enabled=True)
    manager = TrackerManager(config)
    await manager.start()

    try:
        assert manager._tcp_server is not None
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_tcp_server_not_started_when_disabled(instance_patcher):
    config = _make_config(tcp_enabled=False)
    manager = TrackerManager(config)
    await manager.start()

    try:
        assert manager._tcp_server is None
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_stop_clears_tcp_server(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port)
    manager = TrackerManager(config)
    await manager.start()
    assert manager._tcp_server is not None

    await manager.stop()
    assert manager._tcp_server is None


@pytest.mark.asyncio
async def test_auto_provision_disabled(instance_patcher):
    tcp_port = await _get_free_port()
    config = _make_config(tcp_port=tcp_port, auto_provision=False)
    manager = TrackerManager(config)
    await manager.start()

    try:
        _, writer = await _connect(tcp_port)
        await _send_and_drain(
            writer,
            {"node_id": "unknown", "token": TOKEN, "timestamp": 1, "delay": [1.0]},
        )
        await asyncio.sleep(0.1)

        assert len(manager._instances) == 0

        writer.close()
        await writer.wait_closed()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_mixed_http_and_tcp_trackers(instance_patcher):
    tcp_port = await _get_free_port()
    trackers = [
        TrackerConfig(
            name="http-node",
            tcp_port=30090,
            mode="http",
            detection_url="http://example.com/det",
        ),
        TrackerConfig(name="tcp-node", tcp_port=30091, mode="tcp", node_id="tcp-node"),
    ]
    config = _make_config(trackers=trackers, tcp_port=tcp_port)
    manager = TrackerManager(config)
    await manager.start()

    try:
        assert len(manager._instances) == 2

        first_call = instance_patcher.call_args_list[0]
        assert isinstance(first_call[1]["source"], DetectionFetcher)

        second_call = instance_patcher.call_args_list[1]
        assert isinstance(second_call[1]["source"], TcpReceiver)
    finally:
        await manager.stop()
