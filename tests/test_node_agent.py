"""Tests for node_agent."""

import json
import pytest
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

from node_agent import fetch_blah2_config, push_config, post_track_event


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


@pytest.fixture(autouse=False)
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
