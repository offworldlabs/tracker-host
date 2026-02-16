# Node-to-Server Track Posting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current server-polls-nodes architecture with nodes running retina-tracker locally and POSTing track events to a central server via HTTP.

**Architecture:** A thin `node_agent` package runs on each Raspberry Pi node alongside retina-tracker. It starts retina-tracker as a subprocess (which receives detections from blah2 via TCP), reads track events from stdout, and POSTs them to the central `tracker-host` server. The server receives tracks via new aiohttp endpoints, writes them to JSONL files (reusing the existing `OutputHandler`), and runs the geolocator centrally for better multi-radar fusion accuracy. Nodes register dynamically by pushing their blah2 config on startup.

**Tech Stack:** Python 3.10+, aiohttp (server-side HTTP), urllib.request (node-side HTTP, stdlib only), existing retina-tracker and tracker-host infrastructure.

**Key architectural decision:** Dynamic node registration (no pre-configuration needed on the server). Nodes push their radar config to the server, and the server creates per-node output handlers on the fly.

---

## Phase 1: Server-Side HTTP Receive Endpoint

### Task 1: Add ServerConfig to config parsing

**Files:**
- Modify: `tracker_host/config.py`
- Modify: `tests/test_config.py`

**Step 1: Write the failing test**

Add to `tests/test_config.py`:

```python
class TestServerConfig:
    def test_server_defaults(self):
        config = _parse_config({})
        assert config.server.host == "0.0.0.0"
        assert config.server.port == 8080

    def test_server_custom(self):
        raw = {"server": {"host": "127.0.0.1", "port": 9090}}
        config = _parse_config(raw)
        assert config.server.host == "127.0.0.1"
        assert config.server.port == 9090
```

**Step 2: Run test to verify it fails**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_config.py::TestServerConfig -v`
Expected: FAIL with `AttributeError: 'Config' object has no attribute 'server'`

**Step 3: Write minimal implementation**

Add to `tracker_host/config.py`:

```python
@dataclass
class ServerConfig:
    """HTTP server configuration for receiving track events from nodes."""
    host: str = "0.0.0.0"
    port: int = 8080
```

Add `server` field to `Config` dataclass:
```python
server: ServerConfig = field(default_factory=ServerConfig)
```

Add parsing in `_parse_config()` before the `return Config(...)`:
```python
server_raw = raw.get("server", {})
server = ServerConfig(
    host=server_raw.get("host", "0.0.0.0"),
    port=server_raw.get("port", 8080),
)
```

And add `server=server` to the `return Config(...)` call.

**Step 4: Run test to verify it passes**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_config.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add tracker_host/config.py tests/test_config.py
git commit -m "feat: add ServerConfig for HTTP receive endpoint"
```

---

### Task 2: Create NodeRegistry for dynamic node management

**Files:**
- Create: `tracker_host/node_registry.py`
- Create: `tests/test_node_registry.py`

**Step 1: Write the failing tests**

Create `tests/test_node_registry.py`:

```python
"""Tests for NodeRegistry."""

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
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
        import glob
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
```

**Step 2: Run test to verify it fails**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_node_registry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tracker_host.node_registry'`

Note: You may need to install `pytest-asyncio`. Check `requirements.txt` and add if needed:
Run: `pip install pytest-asyncio` (if not already installed)

**Step 3: Write minimal implementation**

Create `tracker_host/node_registry.py`:

```python
"""NodeRegistry: dynamic registration and management of radar nodes."""

import logging
from typing import Any, Optional

from .output_handler import OutputHandler

logger = logging.getLogger(__name__)


class NodeRegistry:
    """Manages dynamically registered radar nodes.

    Nodes register by POSTing their config. Each registered node gets
    an OutputHandler for writing track events to daily JSONL files.
    """

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self._nodes: dict[str, dict[str, Any]] = {}

    async def register_node(self, name: str, config_data: dict) -> None:
        """Register a node or update its config."""
        if name not in self._nodes:
            output_handler = OutputHandler(
                name=name,
                output_dir=self.output_dir,
            )
            self._nodes[name] = {
                "config": config_data,
                "output_handler": output_handler,
            }
            logger.info(f"Registered new node: {name}")
        else:
            self._nodes[name]["config"] = config_data
            logger.info(f"Updated config for node: {name}")

    async def handle_track_event(self, name: str, event_line: str) -> None:
        """Handle a track event from a node."""
        if name not in self._nodes:
            await self.register_node(name, {})

        node = self._nodes[name]
        await node["output_handler"].handle_event(event_line)

    def get_node_config(self, name: str) -> Optional[dict]:
        """Get stored config for a node, or None if not registered."""
        if name in self._nodes:
            return self._nodes[name]["config"]
        return None

    def list_nodes(self) -> dict[str, Any]:
        """List all registered nodes with summary info."""
        return {
            name: {
                "has_config": bool(info["config"]),
                "active_tracks": info["output_handler"].metrics.count,
            }
            for name, info in self._nodes.items()
        }

    async def close(self) -> None:
        """Close all output handlers."""
        for info in self._nodes.values():
            await info["output_handler"].close()
        self._nodes.clear()
```

**Step 4: Run test to verify it passes**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_node_registry.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add tracker_host/node_registry.py tests/test_node_registry.py
git commit -m "feat: add NodeRegistry for dynamic node management"
```

---

### Task 3: Create HTTP receive server

**Files:**
- Create: `tracker_host/server.py`
- Create: `tests/test_server.py`

**Step 1: Write the failing tests**

Create `tests/test_server.py`:

```python
"""Tests for the HTTP receive server."""

import json
import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from tracker_host.server import create_app
from tracker_host.node_registry import NodeRegistry


@pytest.fixture
def registry(tmp_path):
    return NodeRegistry(output_dir=str(tmp_path))


@pytest.fixture
def app(registry):
    return create_app(registry)


@pytest.fixture
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
```

**Step 2: Run test to verify it fails**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_server.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tracker_host.server'`

Note: may need `pip install pytest-aiohttp` for the `aiohttp_client` fixture.

**Step 3: Write minimal implementation**

Create `tracker_host/server.py`:

```python
"""HTTP receive server for accepting track events from remote nodes."""

import json
import logging

from aiohttp import web

from .node_registry import NodeRegistry

logger = logging.getLogger(__name__)


def create_app(registry: NodeRegistry) -> web.Application:
    """Create the aiohttp web application."""
    app = web.Application()
    app["registry"] = registry

    app.router.add_post("/api/node/{name}/config", handle_config)
    app.router.add_post("/api/node/{name}/tracks", handle_tracks)
    app.router.add_get("/api/nodes", handle_list_nodes)

    return app


async def handle_config(request: web.Request) -> web.Response:
    """Receive and store radar config from a node."""
    name = request.match_info["name"]
    registry: NodeRegistry = request.app["registry"]

    try:
        config_data = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"status": "error", "message": "invalid JSON"}, status=400
        )

    await registry.register_node(name, config_data)

    logger.info(f"Received config from node: {name}")
    return web.json_response({"status": "registered", "node": name})


async def handle_tracks(request: web.Request) -> web.Response:
    """Receive track events from a node (JSONL: one event per line)."""
    name = request.match_info["name"]
    registry: NodeRegistry = request.app["registry"]

    body = await request.text()

    for line in body.strip().split("\n"):
        line = line.strip()
        if line:
            await registry.handle_track_event(name, line)

    return web.json_response({"status": "ok"})


async def handle_list_nodes(request: web.Request) -> web.Response:
    """List all registered nodes (for debugging)."""
    registry: NodeRegistry = request.app["registry"]
    return web.json_response({"nodes": registry.list_nodes()})
```

**Step 4: Run test to verify it passes**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_server.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add tracker_host/server.py tests/test_server.py
git commit -m "feat: add HTTP receive server for node track events"
```

---

### Task 4: Wire server mode into __main__.py and config.yaml

**Files:**
- Modify: `tracker_host/__main__.py`
- Modify: `tracker_host/manager.py`
- Modify: `config.yaml`

**Step 1: Update __main__.py to support server mode**

Add `--server` flag. When set, run the HTTP server instead of the polling manager.

In `tracker_host/__main__.py`, update imports and add to the argument parser:

```python
parser.add_argument(
    "--server",
    action="store_true",
    help="Run in server mode (receive tracks from nodes via HTTP)",
)
```

Update the run section:

```python
if args.server:
    from .server_mode import run_server
    asyncio.run(run_server(str(config_path), verbose=args.verbose))
else:
    asyncio.run(run_manager(str(config_path), verbose=args.verbose))
```

**Step 2: Create server_mode.py**

Create `tracker_host/server_mode.py`:

```python
"""Server mode: receive track events from remote nodes via HTTP."""

import asyncio
import logging
import signal

from aiohttp import web

from .config import load_config
from .node_registry import NodeRegistry
from .server import create_app

logger = logging.getLogger(__name__)


async def run_server(config_path: str, verbose: bool = False) -> None:
    """Run tracker-host in server mode."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config(config_path)
    registry = NodeRegistry(output_dir=config.output_dir)
    app = create_app(registry)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, config.server.host, config.server.port)
    await site.start()

    logger.info(
        f"Server listening on http://{config.server.host}:{config.server.port}"
    )

    # Wait for shutdown signal
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.info("Received shutdown signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await stop_event.wait()
    finally:
        await registry.close()
        await runner.cleanup()
        logger.info("Server stopped")
```

**Step 3: Update config.yaml with server section**

Add to the top of `config.yaml`:

```yaml
# Server mode settings (used with --server flag)
server:
  host: "0.0.0.0"
  port: 8080
```

**Step 4: Test manually**

```bash
cd /opt/apps/tracker-host
python -m tracker_host --server -v &
# In another terminal:
curl -s http://localhost:8080/api/nodes | python3 -m json.tool
curl -s -X POST http://localhost:8080/api/node/test/config \
  -H 'Content-Type: application/json' \
  -d '{"location": {"rx": {"lat": 33.9}}}' | python3 -m json.tool
curl -s http://localhost:8080/api/nodes | python3 -m json.tool
# Kill the server
kill %1
```

Expected: First curl returns `{"nodes": {}}`. After POST, second curl shows `test` registered.

**Step 5: Commit**

```bash
git add tracker_host/__main__.py tracker_host/server_mode.py config.yaml
git commit -m "feat: add --server mode for receiving node track POSTs"
```

---

## Phase 2: Node-Side Agent

### Task 5: Create node_agent package

**Files:**
- Create: `node_agent/__init__.py`
- Create: `node_agent/__main__.py`
- Create: `tests/test_node_agent.py`

The node agent uses only stdlib (`urllib.request`, `subprocess`, `json`) so it has no extra dependencies beyond Python itself. It can be deployed to a Pi by copying just the `node_agent/` directory.

**Step 1: Write the failing tests**

Create `tests/test_node_agent.py`:

```python
"""Tests for node_agent."""

import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

from node_agent import push_config, post_track_event, fetch_blah2_config


class MockHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler that records requests."""
    received = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()
        MockHandler.received.append({"path": self.path, "body": body})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok"}).encode())

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        config = {"location": {"rx": {"latitude": 33.9}}, "capture": {"fc": 195000000}}
        self.wfile.write(json.dumps(config).encode())

    def log_message(self, format, *args):
        pass  # Suppress output


@pytest.fixture
def mock_server():
    MockHandler.received = []
    server = HTTPServer(("127.0.0.1", 0), MockHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


class TestNodeAgent:
    def test_fetch_blah2_config(self, mock_server):
        config = fetch_blah2_config(mock_server)
        assert config["location"]["rx"]["latitude"] == 33.9

    def test_push_config(self, mock_server):
        config_data = {"location": {"rx": {"latitude": 33.9}}}
        push_config(mock_server, "radar3", config_data)
        assert len(MockHandler.received) == 1
        assert MockHandler.received[0]["path"] == "/api/node/radar3/config"
        assert json.loads(MockHandler.received[0]["body"]) == config_data

    def test_post_track_event(self, mock_server):
        event = {"track_id": "t-001", "length": 5}
        post_track_event(mock_server, "radar3", json.dumps(event))
        assert len(MockHandler.received) == 1
        assert MockHandler.received[0]["path"] == "/api/node/radar3/tracks"

    def test_post_track_event_failure_does_not_raise(self):
        """POST to unreachable server should log but not crash."""
        event = {"track_id": "t-001", "length": 5}
        # Should not raise
        post_track_event("http://127.0.0.1:1", "radar3", json.dumps(event))
```

**Step 2: Run test to verify it fails**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_node_agent.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'node_agent'`

**Step 3: Write minimal implementation**

Create `node_agent/__init__.py`:

```python
"""Node agent: runs retina-tracker locally and POSTs track events to central server."""

from .agent import fetch_blah2_config, push_config, post_track_event
```

Create `node_agent/agent.py`:

```python
"""Core node agent logic."""

import json
import sys
import urllib.error
import urllib.request


def fetch_blah2_config(blah2_url: str) -> dict:
    """Fetch radar config from local blah2 API."""
    url = f"{blah2_url.rstrip('/')}/api/config"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def push_config(server_url: str, node_name: str, config_data: dict) -> None:
    """POST radar config to the central server."""
    url = f"{server_url.rstrip('/')}/api/node/{node_name}/config"
    data = json.dumps(config_data).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def post_track_event(server_url: str, node_name: str, event_line: str) -> None:
    """POST a single track event to the central server."""
    url = f"{server_url.rstrip('/')}/api/node/{node_name}/tracks"
    data = event_line.encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/x-ndjson"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except (urllib.error.URLError, OSError) as e:
        print(f"POST failed: {e}", file=sys.stderr)
```

**Step 4: Run test to verify it passes**

Run: `cd /opt/apps/tracker-host && python -m pytest tests/test_node_agent.py -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add node_agent/__init__.py node_agent/agent.py tests/test_node_agent.py
git commit -m "feat: add node_agent core HTTP functions"
```

---

### Task 6: Add node agent CLI and subprocess management

**Files:**
- Create: `node_agent/__main__.py`

**Step 1: Write the CLI entry point**

Create `node_agent/__main__.py`:

```python
"""CLI entry point: python -m node_agent"""

import argparse
import signal
import subprocess
import sys
import time

from .agent import fetch_blah2_config, post_track_event, push_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Node agent: run retina-tracker and POST tracks to central server"
    )
    parser.add_argument("--node-name", required=True, help="Unique name for this node")
    parser.add_argument("--server-url", required=True, help="Central server URL")
    parser.add_argument(
        "--blah2-url",
        default="http://localhost:3000",
        help="Local blah2 API URL (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--tracker-path",
        default="../retina-tracker",
        help="Path to retina-tracker directory (default: ../retina-tracker)",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=3012,
        help="TCP port for retina-tracker (default: 3012)",
    )
    parser.add_argument(
        "--tracker-config",
        help="Optional path to retina-tracker config.yaml",
    )

    args = parser.parse_args()

    # Step 1: Fetch config from blah2 and push to server
    print(f"Fetching radar config from {args.blah2_url}...")
    try:
        config = fetch_blah2_config(args.blah2_url)
        print(f"Pushing config to {args.server_url}...")
        push_config(args.server_url, args.node_name, config)
        print("Config registered with server.")
    except Exception as e:
        print(f"Warning: Config push failed: {e}", file=sys.stderr)
        print("Continuing without config push...", file=sys.stderr)

    # Step 2: Start retina-tracker subprocess
    cmd = [
        sys.executable,
        "-m",
        "tracker.track_detections",
        "--tcp",
        "--tcp-host",
        "0.0.0.0",
        "--tcp-port",
        str(args.tcp_port),
        "-s",
        "-",  # Stream events to stdout
    ]

    if args.tracker_config:
        cmd.extend(["-c", args.tracker_config])

    print(f"Starting retina-tracker on TCP port {args.tcp_port}...")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        cwd=args.tracker_path,
    )

    # Handle shutdown gracefully
    def shutdown(signum, frame):
        print("\nShutting down...")
        proc.terminate()
        proc.wait(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Reading track events, POSTing to {args.server_url}...")
    event_count = 0

    # Step 3: Read stdout and POST events
    try:
        for line in proc.stdout:
            decoded = line.decode().strip()
            if decoded:
                post_track_event(args.server_url, args.node_name, decoded)
                event_count += 1
                if event_count % 50 == 0:
                    print(f"  Posted {event_count} track events")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        print(f"Done. Posted {event_count} track events total.")


if __name__ == "__main__":
    main()
```

**Step 2: Test manually (quick sanity check)**

```bash
cd /opt/apps/tracker-host
python -m node_agent --help
```

Expected: Help text printed showing all arguments.

**Step 3: Commit**

```bash
git add node_agent/__main__.py
git commit -m "feat: add node_agent CLI with subprocess management"
```

---

## Phase 3: Testing Infrastructure

### Task 7: Create detection relay for staging tests

On this staging machine, blah2 isn't running locally. This relay script polls a remote detection endpoint (e.g. `radar3.retnode.com/api/detection`) and forwards the data to retina-tracker's TCP port, simulating what blah2 would do on a real node.

**Files:**
- Create: `node_agent/relay.py`

**Step 1: Write the relay**

Create `node_agent/relay.py`:

```python
"""Detection relay: polls HTTP detection endpoint and forwards to retina-tracker TCP.

Used for testing when blah2 isn't running locally. Simulates the blah2→retina-tracker
TCP connection by polling a remote detection API.

Usage:
    python -m node_agent.relay \
        --detection-url https://radar3.retnode.com/api/detection \
        --tcp-port 3012
"""

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Relay: poll detection HTTP endpoint and forward to retina-tracker TCP"
    )
    parser.add_argument(
        "--detection-url",
        default="https://radar3.retnode.com/api/detection",
        help="HTTP detection endpoint to poll",
    )
    parser.add_argument("--tcp-host", default="127.0.0.1", help="Tracker TCP host")
    parser.add_argument("--tcp-port", type=int, default=3012, help="Tracker TCP port")
    parser.add_argument(
        "--interval", type=float, default=0.5, help="Poll interval in seconds"
    )

    args = parser.parse_args()

    # Connect to retina-tracker TCP port
    print(f"Connecting to retina-tracker at {args.tcp_host}:{args.tcp_port}...")
    max_retries = 30
    sock = None
    for attempt in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((args.tcp_host, args.tcp_port))
            break
        except ConnectionRefusedError:
            sock.close()
            sock = None
            if attempt < max_retries - 1:
                print(
                    f"  Connection refused, retrying ({attempt + 1}/{max_retries})..."
                )
                time.sleep(1)

    if sock is None:
        print("Failed to connect to retina-tracker", file=sys.stderr)
        sys.exit(1)

    print(f"Connected! Polling {args.detection_url} every {args.interval}s")
    frame_count = 0

    try:
        while True:
            try:
                req = urllib.request.Request(
                    args.detection_url,
                    headers={"Accept": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = resp.read()

                # Validate it's JSON, then forward as a single line
                json.loads(data)  # Validate
                sock.sendall(data + b"\n")
                frame_count += 1

                if frame_count % 100 == 0:
                    print(f"  Forwarded {frame_count} detection frames")

            except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
                print(f"Error: {e}", file=sys.stderr)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\nDone. Forwarded {frame_count} detection frames.")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
```

**Step 2: Test the relay connects (quick sanity check)**

Don't run the full relay yet (needs retina-tracker running). Just verify it starts:

```bash
cd /opt/apps/tracker-host
python -m node_agent.relay --help
```

Expected: Help text printed.

**Step 3: Commit**

```bash
git add node_agent/relay.py
git commit -m "feat: add detection relay for staging tests"
```

---

### Task 8: End-to-end integration test

This is a manual test that validates the full pipeline. Run each command in a separate terminal.

**Terminal 1: Start the server**

```bash
cd /opt/apps/tracker-host
python -m tracker_host --server -v
```

Expected: `Server listening on http://0.0.0.0:8080`

**Terminal 2: Start the node agent**

```bash
cd /opt/apps/tracker-host
python -m node_agent \
    --node-name radar3 \
    --server-url http://localhost:8080 \
    --blah2-url https://radar3.retnode.com \
    --tracker-path /opt/apps/retina-tracker \
    --tcp-port 3012
```

Expected: Config pushed, retina-tracker started, waiting for TCP connection.

**Terminal 3: Start the detection relay**

```bash
cd /opt/apps/tracker-host
python -m node_agent.relay \
    --detection-url https://radar3.retnode.com/api/detection \
    --tcp-port 3012
```

Expected: Connected, forwarding detection frames.

**Verify output:**

```bash
# After 30-60 seconds, check for JSONL output
ls -la output/radar3_*.jsonl
head -1 output/radar3_*.jsonl | python3 -m json.tool
```

Expected: JSONL file exists with track events containing `track_id`, `detections`, `timestamp`, etc.

```bash
# Check server sees the node
curl -s http://localhost:8080/api/nodes | python3 -m json.tool
```

Expected: `radar3` listed with `has_config: true` and `active_tracks > 0`.

**After successful verification, commit a note:**

```bash
git add -A
git commit -m "feat: complete node-to-server track posting pipeline"
```

---

## Phase 4: Server-Side Geolocator Integration

### Task 9: Wire geolocator to received track events

Once tracks are flowing from nodes to server, the server can run geolocator instances. This reuses the existing `GeolocatorInstance` class.

**Files:**
- Modify: `tracker_host/node_registry.py`
- Modify: `tracker_host/config.py`
- Create: `tests/test_node_registry_geolocator.py`

**Step 1: Add geolocator config to server settings**

In `tracker_host/config.py`, add to `ServerConfig`:

```python
@dataclass
class ServerConfig:
    """HTTP server configuration for receiving track events from nodes."""
    host: str = "0.0.0.0"
    port: int = 8080
    geolocator_enabled: bool = False
    geolocator_tcp_port_base: int = 31000
```

Update parsing in `_parse_config()`:

```python
server = ServerConfig(
    host=server_raw.get("host", "0.0.0.0"),
    port=server_raw.get("port", 8080),
    geolocator_enabled=server_raw.get("geolocator_enabled", False),
    geolocator_tcp_port_base=server_raw.get("geolocator_tcp_port_base", 31000),
)
```

**Step 2: Extend NodeRegistry to manage geolocators**

When a node registers with config (containing `location.rx` and `location.tx`), and geolocator is enabled, the registry:
1. Saves the radar config to a temp YAML file (same as `GeolocatorInstance._fetch_radar_config` does)
2. Creates a `GeolocatorInstance` for that node
3. Forwards track events to the geolocator

In `tracker_host/node_registry.py`, add geolocator support:

```python
import yaml
from pathlib import Path
from .config import Config, GeolocatorConfig
from .geolocator import GeolocatorInstance

class NodeRegistry:
    def __init__(self, output_dir: str, global_config: Optional[Config] = None):
        self.output_dir = output_dir
        self.global_config = global_config
        self._nodes: dict[str, dict[str, Any]] = {}
        self._next_geo_port: int = (
            global_config.server.geolocator_tcp_port_base
            if global_config
            else 31000
        )

    async def register_node(self, name: str, config_data: dict) -> None:
        """Register a node or update its config. Start geolocator if enabled."""
        if name not in self._nodes:
            output_handler = OutputHandler(name=name, output_dir=self.output_dir)
            self._nodes[name] = {
                "config": config_data,
                "output_handler": output_handler,
                "geolocator": None,
            }
            logger.info(f"Registered new node: {name}")
        else:
            self._nodes[name]["config"] = config_data
            logger.info(f"Updated config for node: {name}")

        # Start geolocator if enabled and config has location data
        if (
            self.global_config
            and self.global_config.server.geolocator_enabled
            and config_data.get("location")
            and self._nodes[name]["geolocator"] is None
        ):
            await self._start_geolocator(name, config_data)

    async def _start_geolocator(self, name: str, config_data: dict) -> None:
        """Start a geolocator instance for a node."""
        port = self._next_geo_port
        self._next_geo_port += 1

        # Save radar config to temp file
        config_path = Path(self.output_dir) / f".{name}_config.yml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f, default_flow_style=False)

        geo_config = GeolocatorConfig(enabled=True, tcp_port=port)
        geo = GeolocatorInstance(
            name=name,
            geo_config=geo_config,
            global_config=self.global_config,
            config_url=None,  # Config already saved locally
            session=None,
        )
        # Override the config path since we saved it ourselves
        geo._radar_config_path = str(config_path.resolve())

        try:
            # Skip the fetch step (we already have the config)
            await geo._start_process()
            await geo._connect_tcp()
            geo._running = True
            geo._output_task = asyncio.create_task(
                geo._output_loop(), name=f"{name}-geo-output"
            )
            geo._stderr_task = asyncio.create_task(
                geo._stderr_loop(), name=f"{name}-geo-stderr"
            )
            self._nodes[name]["geolocator"] = geo
            logger.info(f"Started geolocator for {name} on port {port}")
        except Exception as e:
            logger.error(f"Failed to start geolocator for {name}: {e}")

    async def handle_track_event(self, name: str, event_line: str) -> None:
        """Handle a track event from a node."""
        if name not in self._nodes:
            await self.register_node(name, {})

        node = self._nodes[name]
        await node["output_handler"].handle_event(event_line)

        # Forward to geolocator if running
        if node["geolocator"] is not None:
            await node["geolocator"].send_track_event(event_line)
```

**Step 3: Update server_mode.py to pass global_config**

```python
registry = NodeRegistry(output_dir=config.output_dir, global_config=config)
```

**Step 4: Update config.yaml**

```yaml
server:
  host: "0.0.0.0"
  port: 8080
  geolocator_enabled: true
  geolocator_tcp_port_base: 31000
```

**Step 5: Test**

Run the same end-to-end test as Task 8. After tracks flow for 60+ seconds, check for solution output:

```bash
ls -la output/*solutions*.jsonl
```

Expected: Solution JSONL files appear for each node with geolocator enabled.

**Step 6: Commit**

```bash
git add tracker_host/config.py tracker_host/node_registry.py tracker_host/server_mode.py config.yaml
git commit -m "feat: add server-side geolocator for received node tracks"
```

---

## Phase 5: Documentation

### Task 10: Document architecture and update config.yaml

**Files:**
- Modify: `config.yaml` (add documentation comments)
- Modify: `README.md` (add server mode and node agent sections)

**Step 1: Update config.yaml with documentation**

Add clear comments to `config.yaml` explaining both modes and the dynamic node registration decision. Include example commands.

**Step 2: Update README.md**

Add sections for:
- **Server Mode**: How to run with `--server`, what endpoints are available
- **Node Agent**: How to deploy to a Raspberry Pi, example commands
- **Detection Relay**: How to use for staging/testing
- **Architecture**: Brief description of the node→server data flow
- **Architecture Decision: Dynamic Node Registration**: Nodes register by POSTing config, no pre-configuration needed. This simplifies deployment and allows adding/removing nodes without touching the server config.

**Step 3: Commit**

```bash
git add config.yaml README.md
git commit -m "docs: add server mode and node agent documentation"
```

---

## Summary

| Phase | What | Files | Dependencies |
|-------|------|-------|-------------|
| 1 (Tasks 1-4) | Server HTTP endpoint | `config.py`, `node_registry.py`, `server.py`, `server_mode.py`, `__main__.py` | aiohttp (existing) |
| 2 (Tasks 5-6) | Node agent | `node_agent/agent.py`, `node_agent/__main__.py` | stdlib only |
| 3 (Tasks 7-8) | Testing infrastructure | `node_agent/relay.py` + manual E2E test | stdlib only |
| 4 (Task 9) | Server geolocator | `node_registry.py` (extend), `config.py` | existing GeolocatorInstance |
| 5 (Task 10) | Documentation | `config.yaml`, `README.md` | — |

**Deployment to a real Pi node requires only:**
1. `retina-tracker/` (already needed)
2. `node_agent/` directory (3 small Python files, no extra deps)
3. One command: `python -m node_agent --node-name radar3 --server-url https://server.example.com`
