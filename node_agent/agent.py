"""Core node agent logic."""

import json
import sys
import urllib.error
import urllib.request

_HEADERS = {"User-Agent": "retina-node-agent/1.0"}


def fetch_blah2_config(blah2_url: str) -> dict:
    """Fetch radar config from local blah2 API."""
    url = f"{blah2_url.rstrip('/')}/api/config"
    req = urllib.request.Request(url, headers={**_HEADERS, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def push_config(server_url: str, node_name: str, config_data: dict) -> None:
    """POST radar config to the central server."""
    url = f"{server_url.rstrip('/')}/api/node/{node_name}/config"
    data = json.dumps(config_data).encode()
    req = urllib.request.Request(
        url, data=data, headers={**_HEADERS, "Content-Type": "application/json"}
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
        headers={**_HEADERS, "Content-Type": "application/x-ndjson"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except (urllib.error.URLError, OSError) as e:
        print(f"POST failed: {e}", file=sys.stderr)
