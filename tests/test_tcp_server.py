"""Tests for TcpServer."""

import asyncio
import json

import pytest
import pytest_asyncio

from tracker_host.tcp_receiver import TcpReceiver
from tracker_host.tcp_server import TcpServer

TOKEN = "test-secret"


async def _get_free_port():
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port


async def _connect(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    return reader, writer


def _send_line(writer, msg):
    writer.write((json.dumps(msg) + "\n").encode())


async def _send_and_drain(writer, msg):
    _send_line(writer, msg)
    await writer.drain()


@pytest_asyncio.fixture
async def server_and_port():
    port = await _get_free_port()
    server = TcpServer(token=TOKEN)
    await server.start("127.0.0.1", port)
    yield server, port
    await server.stop()


@pytest.mark.asyncio
async def test_accepts_connections(server_and_port):
    server, port = server_and_port
    reader, writer = await _connect(port)
    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "type": "heartbeat"}
    )
    await asyncio.sleep(0.05)
    assert not writer.is_closing()
    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_validates_token(server_and_port):
    server, port = server_and_port
    receiver = TcpReceiver("n1")
    server.register_receiver("n1", receiver)

    reader, writer = await _connect(port)
    frame = {"node_id": "n1", "token": TOKEN, "timestamp": 100, "delay": [1.0]}
    await _send_and_drain(writer, frame)
    await asyncio.sleep(0.05)

    result = await receiver.receive()
    assert result is not None
    assert result["timestamp"] == 100

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_rejects_invalid_token(server_and_port):
    server, port = server_and_port
    reader, writer = await _connect(port)
    await _send_and_drain(writer, {"node_id": "n1", "token": "wrong"})
    await asyncio.sleep(0.1)

    data = await asyncio.wait_for(reader.read(1024), timeout=1.0)
    assert data == b""
    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_routes_frames_to_correct_receiver(server_and_port):
    server, port = server_and_port
    r1 = TcpReceiver("n1")
    r2 = TcpReceiver("n2")
    server.register_receiver("n1", r1)
    server.register_receiver("n2", r2)

    _, w1 = await _connect(port)
    _, w2 = await _connect(port)

    await _send_and_drain(
        w1, {"node_id": "n1", "token": TOKEN, "timestamp": 1, "delay": [1.0]}
    )
    await _send_and_drain(
        w2, {"node_id": "n2", "token": TOKEN, "timestamp": 2, "delay": [2.0]}
    )
    await asyncio.sleep(0.05)

    result1 = await r1.receive()
    result2 = await r2.receive()

    assert result1["timestamp"] == 1
    assert result2["timestamp"] == 2

    w1.close()
    w2.close()
    await w1.wait_closed()
    await w2.wait_closed()


@pytest.mark.asyncio
async def test_discards_heartbeat_messages(server_and_port):
    server, port = server_and_port
    receiver = TcpReceiver("n1")
    server.register_receiver("n1", receiver)

    _, writer = await _connect(port)
    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "type": "heartbeat"}
    )
    await asyncio.sleep(0.05)

    result = await receiver.receive()
    assert result is None

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_calls_provision_callback_for_unknown_node(server_and_port):
    server, port = server_and_port
    provisioned = []

    def on_provision(node_id, frame):
        receiver = TcpReceiver(node_id)
        server.register_receiver(node_id, receiver)
        provisioned.append((node_id, frame))

    server.set_provision_callback(on_provision)

    _, writer = await _connect(port)
    frame = {"node_id": "new-node", "token": TOKEN, "timestamp": 500, "delay": [3.0]}
    await _send_and_drain(writer, frame)
    await asyncio.sleep(0.05)

    assert len(provisioned) == 1
    assert provisioned[0][0] == "new-node"
    assert provisioned[0][1]["timestamp"] == 500

    result = await server._receivers["new-node"].receive()
    assert result is not None
    assert result["timestamp"] == 500

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_disconnect_sets_receiver_unhealthy(server_and_port):
    server, port = server_and_port
    receiver = TcpReceiver("n1")
    server.register_receiver("n1", receiver)

    _, writer = await _connect(port)
    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "timestamp": 1, "delay": [1.0]}
    )
    await asyncio.sleep(0.05)

    assert receiver.is_healthy is True

    writer.close()
    await writer.wait_closed()
    await asyncio.sleep(0.1)

    assert receiver.is_healthy is False


@pytest.mark.asyncio
async def test_enforces_max_message_size(server_and_port):
    server, port = server_and_port
    receiver = TcpReceiver("n1")
    server.register_receiver("n1", receiver)

    _, writer = await _connect(port)
    await _send_and_drain(writer, {"node_id": "n1", "token": TOKEN, "timestamp": 1})
    await asyncio.sleep(0.05)

    oversized = json.dumps(
        {"node_id": "n1", "token": TOKEN, "timestamp": 999, "data": "x" * 1_100_000}
    )
    chunk_size = 32768
    payload = (oversized + "\n").encode()
    for i in range(0, len(payload), chunk_size):
        writer.write(payload[i : i + chunk_size])
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.5)

    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "timestamp": 2, "delay": [1.0]}
    )
    await asyncio.sleep(0.1)

    frames = []
    while True:
        r = await receiver.receive()
        if r is None:
            break
        frames.append(r)

    timestamps = [f["timestamp"] for f in frames]
    assert 999 not in timestamps
    assert 2 in timestamps

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_handles_malformed_json(server_and_port):
    server, port = server_and_port
    receiver = TcpReceiver("n1")
    server.register_receiver("n1", receiver)

    _, writer = await _connect(port)
    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "timestamp": 1, "delay": [1.0]}
    )
    await asyncio.sleep(0.05)

    writer.write(b"not valid json\n")
    await writer.drain()
    await asyncio.sleep(0.05)

    await _send_and_drain(
        writer, {"node_id": "n1", "token": TOKEN, "timestamp": 2, "delay": [2.0]}
    )
    await asyncio.sleep(0.05)

    r1 = await receiver.receive()
    assert r1["timestamp"] == 1
    r2 = await receiver.receive()
    assert r2["timestamp"] == 2

    writer.close()
    await writer.wait_closed()
