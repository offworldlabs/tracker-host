"""Tests for TcpReceiver."""

import pytest

from tracker_host.tcp_receiver import MAX_QUEUE_SIZE, TcpReceiver


@pytest.fixture
def receiver():
    return TcpReceiver(node_id="test-node")


@pytest.mark.asyncio
async def test_receive_returns_frame_from_queue(receiver):
    frame = {"timestamp": 1000, "delay": [1.0], "doppler": [2.0]}
    receiver.put(frame)
    result = await receiver.receive()
    assert result == frame


@pytest.mark.asyncio
async def test_receive_returns_none_on_empty_queue(receiver):
    result = await receiver.receive()
    assert result is None


@pytest.mark.asyncio
async def test_timestamp_dedup_skips_duplicate(receiver):
    frame1 = {"timestamp": 1000, "delay": [1.0]}
    frame2 = {"timestamp": 1000, "delay": [2.0]}
    frame3 = {"timestamp": 2000, "delay": [3.0]}

    receiver.put(frame1)
    receiver.put(frame2)
    receiver.put(frame3)

    result1 = await receiver.receive()
    assert result1 == frame1

    result2 = await receiver.receive()
    assert result2 is None

    result3 = await receiver.receive()
    assert result3 == frame3


@pytest.mark.asyncio
async def test_put_drops_oldest_when_queue_full(receiver):
    for i in range(MAX_QUEUE_SIZE):
        receiver.put({"timestamp": i})

    assert receiver._queue.full()

    receiver.put({"timestamp": 9999})

    assert receiver._queue.qsize() == MAX_QUEUE_SIZE

    first = receiver._queue.get_nowait()
    assert first["timestamp"] == 1


@pytest.mark.asyncio
async def test_is_healthy_tracks_connected_state(receiver):
    assert receiver.is_healthy is False

    receiver.set_connected(True)
    assert receiver.is_healthy is True

    receiver.set_connected(False)
    assert receiver.is_healthy is False


@pytest.mark.asyncio
async def test_close_clears_queue(receiver):
    receiver.put({"timestamp": 1})
    receiver.put({"timestamp": 2})

    await receiver.close()
    assert receiver._queue.empty()


@pytest.mark.asyncio
async def test_frame_without_timestamp_always_returned(receiver):
    frame = {"delay": [1.0]}
    receiver.put(frame)
    result = await receiver.receive()
    assert result == frame
