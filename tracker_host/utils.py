"""Shared utility functions for tracker-host."""

import asyncio
import logging
import socket
from pathlib import Path
from typing import AsyncIterator, Optional

logger = logging.getLogger(__name__)


def increment_filename(path: str) -> str:
    """Return path with incremented numeric suffix to avoid overwriting.

    radar3_tracks.png -> radar3_tracks_001.png (if radar3_tracks.png exists)
    radar3_tracks_001.png -> radar3_tracks_002.png (if _001 exists)
    """
    p = Path(path)
    if not p.exists():
        return str(p)
    stem, suffix = p.stem, p.suffix
    parent = p.parent
    n = 1
    while True:
        candidate = parent / f"{stem}_{n:03d}{suffix}"
        if not candidate.exists():
            return str(candidate)
        n += 1


def check_port_available(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a port is available for binding."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            return True
    except OSError:
        return False


async def connect_tcp(
    host: str,
    port: int,
    max_retries: int = 10,
    retry_delay: float = 0.5,
    label: str = "",
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Connect to a TCP port with retries.

    Args:
        host: Host to connect to.
        port: Port to connect to.
        max_retries: Maximum number of connection attempts.
        retry_delay: Delay between retries in seconds.
        label: Label for log messages (e.g. "tracker" or "geolocator").

    Returns:
        Tuple of (reader, writer).

    Raises:
        RuntimeError: If all connection attempts fail.
    """
    for attempt in range(max_retries):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            logger.info(f"Connected to {label} TCP port {port}")
            return reader, writer
        except (ConnectionRefusedError, OSError) as e:
            if attempt < max_retries - 1:
                logger.debug(
                    f"{label} TCP connection attempt {attempt + 1} failed: {e}, retrying..."
                )
                await asyncio.sleep(retry_delay)
            else:
                raise RuntimeError(
                    f"Failed to connect to {label} after {max_retries} attempts"
                )


async def start_subprocess(
    cmd: list[str],
    cwd: Path,
    startup_delay: float = 1.0,
    label: str = "",
) -> asyncio.subprocess.Process:
    """Start a subprocess with stdout/stderr pipes and verify it started.

    Args:
        cmd: Command and arguments.
        cwd: Working directory.
        startup_delay: Seconds to wait after spawning before checking status.
        label: Label for log messages.

    Returns:
        The running subprocess.

    Raises:
        RuntimeError: If the process exits immediately.
    """
    logger.info(f"Starting {label}: {' '.join(cmd)}")

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )

    await asyncio.sleep(startup_delay)

    if process.returncode is not None:
        stderr = await process.stderr.read()
        raise RuntimeError(f"{label} process exited immediately: {stderr.decode()}")

    logger.info(f"{label} process started (PID: {process.pid})")
    return process


async def stop_subprocess(
    process: Optional[asyncio.subprocess.Process],
    timeout: float = 5.0,
    label: str = "",
) -> None:
    """Stop a subprocess gracefully, force-killing after timeout.

    Args:
        process: The process to stop (may be None).
        timeout: Seconds to wait for graceful termination.
        label: Label for log messages.
    """
    if process is None:
        return

    if process.returncode is None:
        logger.info(f"Terminating {label} process")
        process.terminate()

        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Force killing {label} process")
            process.kill()
            await process.wait()


async def read_stream_lines(
    stream: Optional[asyncio.StreamReader],
    timeout: float = 1.0,
) -> AsyncIterator[str]:
    """Async generator that yields decoded lines from a stream with timeout.

    Args:
        stream: An asyncio StreamReader (e.g. process.stdout or process.stderr).
        timeout: Seconds to wait for each readline before yielding nothing.

    Yields:
        Decoded line strings (non-empty lines only).
    """
    if stream is None:
        return

    try:
        line = await asyncio.wait_for(stream.readline(), timeout=timeout)
        if line:
            yield line.decode()
    except asyncio.TimeoutError:
        return
