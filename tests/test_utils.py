"""Tests for tracker_host.utils."""

import asyncio
import socket
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from tracker_host.utils import (
    check_port_available,
    connect_tcp,
    increment_filename,
    start_subprocess,
    stop_subprocess,
)


class TestIncrementFilename:
    def test_no_conflict(self, tmp_path):
        path = str(tmp_path / "output.png")
        assert increment_filename(path) == path

    def test_first_conflict(self, tmp_path):
        existing = tmp_path / "output.png"
        existing.touch()
        result = increment_filename(str(existing))
        assert result == str(tmp_path / "output_001.png")

    def test_multiple_conflicts(self, tmp_path):
        (tmp_path / "output.png").touch()
        (tmp_path / "output_001.png").touch()
        (tmp_path / "output_002.png").touch()
        result = increment_filename(str(tmp_path / "output.png"))
        assert result == str(tmp_path / "output_003.png")

    def test_preserves_extension(self, tmp_path):
        (tmp_path / "data.jsonl").touch()
        result = increment_filename(str(tmp_path / "data.jsonl"))
        assert result.endswith("_001.jsonl")


class TestCheckPortAvailable:
    def test_available_port(self):
        assert check_port_available(0) is True

    def test_unavailable_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            assert check_port_available(port) is False


class TestConnectTcp:
    @pytest.mark.asyncio
    async def test_connect_success(self):
        server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        try:
            reader, writer = await connect_tcp(
                "127.0.0.1", port, max_retries=2, retry_delay=0.1, label="test"
            )
            writer.close()
            await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        with pytest.raises(RuntimeError, match="Failed to connect"):
            await connect_tcp(
                "127.0.0.1", 1, max_retries=2, retry_delay=0.05, label="test"
            )


class TestStopSubprocess:
    @pytest.mark.asyncio
    async def test_stop_none(self):
        await stop_subprocess(None, label="test")

    @pytest.mark.asyncio
    async def test_stop_already_exited(self):
        proc = AsyncMock()
        proc.returncode = 0
        await stop_subprocess(proc, label="test")
        proc.terminate.assert_not_called()
