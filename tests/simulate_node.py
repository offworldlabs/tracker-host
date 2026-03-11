#!/usr/bin/env python3
import argparse
import json
import random
import socket
import sys
import threading
import time


def generate_detection_frame(node_id, token, timestamp):
    num_detections = random.randint(1, 5)
    return {
        "node_id": node_id,
        "token": token,
        "timestamp": timestamp,
        "delay": [round(random.uniform(10, 300), 2) for _ in range(num_detections)],
        "doppler": [round(random.uniform(-150, 150), 2) for _ in range(num_detections)],
        "snr": [round(random.uniform(5, 25), 2) for _ in range(num_detections)],
        "adsb": [None] * num_detections,
    }


def generate_heartbeat(node_id, token):
    return {"node_id": node_id, "token": token, "type": "heartbeat"}


def check_connection(sock):
    """Non-blocking check if server has closed the connection."""
    sock.setblocking(False)
    try:
        data = sock.recv(1)
        if not data:
            raise ConnectionResetError("Server closed connection")
    except BlockingIOError:
        pass  # No data available = connection still open
    finally:
        sock.setblocking(True)


def send_message(sock, msg, verbose=False, label=""):
    line = json.dumps(msg) + "\n"
    try:
        sock.sendall(line.encode("utf-8"))
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        if verbose:
            print(f"[{label}] Send failed: {e}")
        raise
    if verbose:
        print(f"[{label}] Sent: {line.rstrip()}")


def run_node(args, node_id):
    label = node_id
    token = "wrong-token" if args.invalid_token else args.token
    frames_sent = 0
    timestamp = int(time.time() * 1000)

    while True:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            print(f"[{label}] Connecting to {args.host}:{args.port}...")
            sock.connect((args.host, args.port))
            print(f"[{label}] Connected")

            if args.malformed:
                garbage = b"\xff\xfe NOT JSON {{{\x00\n"
                sock.sendall(garbage)
                if args.verbose:
                    print(f"[{label}] Sent malformed data")
                time.sleep(1)
                return

            if args.buffer_burst and frames_sent > 0:
                burst_count = args.buffer_burst
                print(f"[{label}] Draining buffer: {burst_count} frames")
                for _ in range(burst_count):
                    frame = generate_detection_frame(node_id, token, timestamp)
                    send_message(sock, frame, args.verbose, label)
                    timestamp += int(args.cpi * 1000)
                    frames_sent += 1
                    if args.num_frames and frames_sent >= args.num_frames:
                        print(f"[{label}] Reached {args.num_frames} frames, stopping")
                        return

            last_send_time = time.time()
            frames_this_connection = 0

            while True:
                if args.num_frames and frames_sent >= args.num_frames:
                    print(f"[{label}] Reached {args.num_frames} frames, stopping")
                    return

                if (
                    args.disconnect_after
                    and frames_this_connection >= args.disconnect_after
                ):
                    print(
                        f"[{label}] Disconnecting after {args.disconnect_after} frames"
                    )
                    sock.close()
                    sock = None
                    delay = args.reconnect_delay
                    print(f"[{label}] Waiting {delay}s before reconnect...")
                    time.sleep(delay)
                    break

                now = time.time()

                if args.heartbeat_only:
                    if now - last_send_time >= 10:
                        hb = generate_heartbeat(node_id, token)
                        send_message(sock, hb, args.verbose, label)
                        last_send_time = now
                        frames_sent += 1
                        frames_this_connection += 1
                    time.sleep(0.5)
                    continue

                frame = generate_detection_frame(node_id, token, timestamp)
                send_message(sock, frame, args.verbose, label)
                timestamp += int(args.cpi * 1000)
                frames_sent += 1
                frames_this_connection += 1
                last_send_time = now

                time.sleep(args.cpi)
                check_connection(sock)

        except ConnectionRefusedError:
            print(
                f"[{label}] Connection refused, retrying in {args.reconnect_delay}s..."
            )
            time.sleep(args.reconnect_delay)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"[{label}] Connection lost: {e}")
            if args.invalid_token:
                print(f"[{label}] Server likely rejected invalid token")
                return
            print(f"[{label}] Reconnecting in {args.reconnect_delay}s...")
            time.sleep(args.reconnect_delay)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def main():
    parser = argparse.ArgumentParser(
        description="Simulate radar nodes pushing detection frames over TCP"
    )
    parser.add_argument(
        "--host", default="localhost", help="Server hostname (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=30050, help="Server port (default: 30050)"
    )
    parser.add_argument(
        "--node-id", default="radar-test", help="Node ID to send (default: radar-test)"
    )
    parser.add_argument(
        "--token", default="secret", help="Auth token (default: secret)"
    )
    parser.add_argument(
        "--cpi",
        type=float,
        default=0.5,
        help="Interval between frames in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=0,
        help="Simulate N concurrent nodes (node IDs: node-1, node-2, etc.)",
    )
    parser.add_argument(
        "--disconnect-after",
        type=int,
        default=0,
        help="Disconnect after N frames (default: 0 = never)",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=5,
        help="Seconds to wait before reconnecting (default: 5)",
    )
    parser.add_argument(
        "--buffer-burst",
        type=int,
        default=0,
        help="Send N frames rapidly on reconnect (simulating buffer drain)",
    )
    parser.add_argument(
        "--invalid-token",
        action="store_true",
        help="Use wrong token (for testing rejection)",
    )
    parser.add_argument(
        "--malformed", action="store_true", help="Send garbage data instead of JSON"
    )
    parser.add_argument(
        "--heartbeat-only",
        action="store_true",
        help="Send only heartbeats, no detections",
    )
    parser.add_argument(
        "--num-frames",
        type=int,
        default=0,
        help="Stop after N frames (default: 0 = unlimited)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print each frame sent")

    args = parser.parse_args()

    if args.nodes > 1:
        threads = []
        for i in range(1, args.nodes + 1):
            nid = f"node-{i}"
            t = threading.Thread(target=run_node, args=(args, nid), daemon=True)
            t.start()
            threads.append(t)
            print(f"Started thread for {nid}")

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            print("\nInterrupted, shutting down...")
            sys.exit(0)
    else:
        try:
            run_node(args, args.node_id)
        except KeyboardInterrupt:
            print("\nInterrupted, shutting down...")
            sys.exit(0)


if __name__ == "__main__":
    main()
