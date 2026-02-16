# tracker-host

Multi-instance tracker manager for [retina-tracker](https://github.com/offworldlabs/retina-tracker). Supports two operating modes: **manager mode** (polls remote detection endpoints centrally) and **server mode** (receives track events pushed from remote nodes).

## Overview

tracker-host is a Python service that manages multiple retina-tracker instances. It operates in one of two modes:

### Manager Mode (default)

The server polls detection endpoints on remote radar nodes and manages tracker subprocesses locally. For each configured detection endpoint, it:

1. Fetches detection data from the endpoint at ~2 Hz
2. Feeds data to a dedicated retina-tracker subprocess via TCP
3. Reads tracker output (streaming JSONL) and saves to daily files
4. Optionally forwards track events to a configurable API in real-time

### Server Mode (`--server`)

Remote radar nodes run retina-tracker locally and POST track events to the central server via HTTP. The server receives tracks, writes them to JSONL files, and optionally runs geolocator instances to convert tracks to geographic solutions. Nodes self-register by POSTing their config -- no pre-configuration is needed on the server side.

## Architecture

### Manager Mode

```
┌─────────────────────────────────────────────────────────────┐
│                      tracker-host                           │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │ TrackerInstance │  │ TrackerInstance │  ... (up to 50)  │
│  │                 │  │                 │                  │
│  │ - HTTP Fetcher  │  │ - HTTP Fetcher  │                  │
│  │ - TCP Client    │  │ - TCP Client    │                  │
│  │ - Output Reader │  │ - Output Reader │                  │
│  │ - File Writer   │  │ - File Writer   │                  │
│  └────────┬────────┘  └────────┬────────┘                  │
│           │                    │                           │
│           ▼                    ▼                           │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │ retina-tracker  │  │ retina-tracker  │  (subprocesses)  │
│  │ (TCP :30012)    │  │ (TCP :30013)    │                  │
│  └─────────────────┘  └─────────────────┘                  │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Metrics/Status: track counts, track lengths, health    ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### Server Mode

```
Node (Raspberry Pi)                         Central Server
┌───────────────────────────┐              ┌──────────────────────────────────┐
│                           │              │          tracker-host --server   │
│  blah2 (radar)            │              │                                  │
│    │                      │              │  ┌────────────┐                  │
│    ▼ TCP                  │              │  │ HTTP Server │ :8080           │
│  retina-tracker           │              │  │ (aiohttp)  │                  │
│    │                      │  HTTP POST   │  └─────┬──────┘                  │
│    ▼ stdout JSONL         │ ──────────── │        │                         │
│  node_agent               │  /api/node/  │        ▼                         │
│    (POST tracks + config) │  {name}/...  │  ┌──────────────┐                │
│                           │              │  │ NodeRegistry  │                │
└───────────────────────────┘              │  └──┬───────┬───┘                │
                                           │     │       │                    │
                                           │     ▼       ▼                    │
                                           │  Output   GeolocatorInstance    │
                                           │  Handler  (optional lat/lon/alt) │
                                           │  (JSONL)                         │
                                           └──────────────────────────────────┘
```

## Installation

```bash
# Clone the repository
git clone https://github.com/offworldlabs/tracker-host.git
cd tracker-host

# Install dependencies
pip install -r requirements.txt

# Ensure retina-tracker is available (default: ../retina-tracker)
```

## Usage

### Manager Mode (default)

```bash
# Run with default config
python -m tracker_host

# Run with custom config
python -m tracker_host -c /path/to/config.yaml

# Run with verbose logging
python -m tracker_host -v
```

### Server Mode

Start the server to receive track events from remote nodes:

```bash
# Run in server mode (listens on 0.0.0.0:8080 by default)
python -m tracker_host --server

# Server mode with verbose logging
python -m tracker_host --server -v

# Server mode with custom config
python -m tracker_host --server -c /path/to/config.yaml
```

Server mode configuration is specified in the `server` section of `config.yaml`:

```yaml
server:
  host: "0.0.0.0"
  port: 8080
  geolocator_enabled: false
  geolocator_tcp_port_base: 31000
```

| Setting | Default | Description |
|---------|---------|-------------|
| `host` | `0.0.0.0` | Bind address for the HTTP server |
| `port` | `8080` | Port for the HTTP server |
| `geolocator_enabled` | `false` | Start geolocator instances for nodes that provide location data |
| `geolocator_tcp_port_base` | `31000` | Starting port number for geolocator TCP connections (auto-increments per node) |

#### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/node/{name}/config` | Register a node and push its radar config (JSON body) |
| `POST` | `/api/node/{name}/tracks` | Push track events from a node (JSONL body, one event per line) |
| `GET`  | `/api/nodes` | List all registered nodes with status info |

The `{name}` parameter must match `[a-zA-Z0-9_-]+`.

Example: register a node and push tracks:

```bash
# Push radar config for a node
curl -X POST http://server:8080/api/node/radar3/config \
  -H "Content-Type: application/json" \
  -d '{"location": {"rx_latitude": -36.8, "rx_longitude": 174.7, "rx_altitude": 50}}'

# Push a track event
curl -X POST http://server:8080/api/node/radar3/tracks \
  -H "Content-Type: application/x-ndjson" \
  -d '{"track_id":"260120-000000","timestamp":1768932173636,"length":3,"detections":[...]}'

# List registered nodes
curl http://server:8080/api/nodes
```

### Node Agent

The `node_agent/` package runs on Raspberry Pi 5 nodes (or any machine with blah2 and retina-tracker). It is a **stdlib-only** Python package with no external dependencies, designed for easy deployment to resource-constrained devices.

The node agent:

1. Fetches the radar config from the local blah2 instance and pushes it to the central server
2. Starts retina-tracker as a subprocess, listening for detections on a TCP port
3. Reads track events from retina-tracker stdout (streaming JSONL) and POSTs each event to the central server

#### Deploying to a Raspberry Pi

Copy the `node_agent/` directory and ensure `retina-tracker` is available on the node:

```bash
# On the Pi (assuming retina-tracker is at ../retina-tracker)
python3 -m node_agent \
  --node-name radar3 \
  --server-url http://central-server:8080 \
  --blah2-url http://localhost:3000 \
  --tracker-path ../retina-tracker \
  --tcp-port 3012
```

| Flag | Default | Description |
|------|---------|-------------|
| `--node-name` | (required) | Unique name for this node (used in API paths and output filenames) |
| `--server-url` | (required) | URL of the central tracker-host server |
| `--blah2-url` | `http://localhost:3000` | URL of the local blah2 radar API |
| `--tracker-path` | `../retina-tracker` | Path to the retina-tracker directory |
| `--tcp-port` | `3012` | TCP port for the retina-tracker subprocess |
| `--tracker-config` | (none) | Optional path to a retina-tracker config.yaml |

The node agent uses only the Python standard library (`urllib`, `subprocess`, `socket`, `json`), so no `pip install` step is needed on the Pi.

### Detection Relay (staging/testing)

The detection relay (`node_agent/relay.py`) is for staging and testing when blah2 is not running locally on the node. It polls a remote HTTP detection endpoint and forwards the data to retina-tracker over TCP, simulating the blah2-to-retina-tracker connection.

```bash
# Start retina-tracker first (in another terminal), then run the relay:
python3 -m node_agent.relay \
  --detection-url https://radar3.retnode.com/api/detection \
  --tcp-host 127.0.0.1 \
  --tcp-port 3012 \
  --interval 0.5
```

| Flag | Default | Description |
|------|---------|-------------|
| `--detection-url` | `https://radar3.retnode.com/api/detection` | HTTP detection endpoint to poll |
| `--tcp-host` | `127.0.0.1` | retina-tracker TCP host |
| `--tcp-port` | `3012` | retina-tracker TCP port |
| `--interval` | `0.5` | Poll interval in seconds |

The relay retries the TCP connection up to 30 times (1 second apart), so you can start it before retina-tracker is ready.

### Architecture Decision: Dynamic Node Registration

Nodes self-register with the central server by POSTing their config to `/api/node/{name}/config`. This means:

- **No pre-configuration needed on the server.** The server does not need a list of nodes in its config file. Nodes appear automatically when they connect.
- **Adding or removing nodes requires no server restart.** Deploy a new Pi, point it at the server, and it registers itself.
- **Each node controls its own identity** via the `--node-name` flag.

If a node POSTs config again, the existing registration is updated rather than duplicated.

## Configuration

Edit `config.yaml` to configure tracker instances:

```yaml
# Global settings
output_dir: "./output"
poll_interval_sec: 0.5
status_interval_sec: 30.0

# Path to retina-tracker
retina_tracker_path: "../retina-tracker"

# Retry/resilience settings
retry:
  max_attempts: 5
  backoff_base_sec: 2
  extended_outage_sec: 60
  health_check_interval_sec: 30

# Optional API forwarding
api_forward:
  enabled: false
  url: ""

# Tracker instances
trackers:
  - name: "radar3"
    detection_url: "https://radar3.example.com/api/detection"
    tcp_port: 30012

  - name: "radar4"
    detection_url: "https://radar4.example.com/api/detection"
    tcp_port: 30013
```

## Output

Track events are saved to daily JSONL files:

```
output/radar3_2026-01-20.jsonl
output/radar4_2026-01-20.jsonl
```

Each line is a JSON object containing track data:

```json
{
  "track_id": "260120-000000",
  "adsb_hex": null,
  "adsb_initialized": false,
  "timestamp": 1768932173636,
  "length": 3,
  "detections": [
    {"timestamp": 1768932171274, "delay": 40.46, "doppler": -27.99, "snr": 12.08},
    {"timestamp": 1768932172478, "delay": 40.86, "doppler": -28.52, "snr": 14.46}
  ],
  "is_anomalous": false,
  "max_velocity_ms": 0.0
}
```

## Plotting

Generate delay-Doppler plots from output files:

```bash
# Plot tracks from output file
python -m tracker_host.plotter output/radar3_2026-01-20.jsonl -o tracks.png

# Filter to longer tracks only
python -m tracker_host.plotter output/radar3_2026-01-20.jsonl --min-length 10
```

## Features

### Manager Mode
- **Multi-instance management**: Run multiple tracker instances from a single process
- **Resilience**: Exponential backoff on failures, auto-restart after extended outages
- **Port collision detection**: Checks port availability before starting trackers
- **Real-time status**: Periodic console output showing track counts and lengths
- **API forwarding**: Optional real-time forwarding of track events to external APIs

### Server Mode
- **Dynamic node registration**: Nodes self-register by POSTing config; no server-side pre-configuration
- **HTTP track ingestion**: Receives JSONL track events via POST from remote nodes
- **Automatic geolocator**: Optionally starts geolocator instances for nodes with location data
- **Node listing API**: Query registered nodes and their status via REST

### Shared
- **Daily output files**: Automatic file rotation at midnight
- **Graceful shutdown**: Clean termination on SIGINT/SIGTERM

## Requirements

- Python 3.10+
- aiohttp >= 3.9.0
- pyyaml >= 6.0
- matplotlib >= 3.8.0 (for plotting)
- retina-tracker (as a sibling directory or configured path)

## License

MIT
