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
