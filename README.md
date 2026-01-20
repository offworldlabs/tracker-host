# tracker-host

Multi-instance tracker manager for [retina-tracker](https://github.com/offworldlabs/retina-tracker). Fetches detection data from multiple radar endpoints and manages tracker subprocesses.

## Overview

tracker-host is a Python service that manages multiple retina-tracker instances concurrently. For each configured detection endpoint, it:

1. Fetches detection data from the endpoint at ~2 Hz
2. Feeds data to a dedicated retina-tracker subprocess via TCP
3. Reads tracker output (streaming JSONL) and saves to daily files
4. Optionally forwards track events to a configurable API in real-time

## Architecture

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

```bash
# Run with default config
python -m tracker_host

# Run with custom config
python -m tracker_host -c /path/to/config.yaml

# Run with verbose logging
python -m tracker_host -v
```

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

- **Multi-instance management**: Run multiple tracker instances from a single process
- **Resilience**: Exponential backoff on failures, auto-restart after extended outages
- **Port collision detection**: Checks port availability before starting trackers
- **Daily output files**: Automatic file rotation at midnight
- **Real-time status**: Periodic console output showing track counts and lengths
- **API forwarding**: Optional real-time forwarding of track events to external APIs
- **Graceful shutdown**: Clean termination on SIGINT/SIGTERM

## Requirements

- Python 3.10+
- aiohttp >= 3.9.0
- pyyaml >= 6.0
- matplotlib >= 3.8.0 (for plotting)
- retina-tracker (as a sibling directory or configured path)

## License

MIT
