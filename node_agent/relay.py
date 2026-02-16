"""Detection relay: polls HTTP detection endpoint and forwards to retina-tracker TCP.

Used for testing when blah2 isn't running locally. Simulates the blah2->retina-tracker
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
