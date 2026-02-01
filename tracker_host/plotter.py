"""Plot tracks from tracker-host JSONL output."""

import argparse
import json

import matplotlib.pyplot as plt
import numpy as np


def load_tracks(filepath):
    """Load track events from JSONL file.

    Returns dict: {track_id: {'delays': [], 'dopplers': [], 'snrs': []}}
    Only keeps the final (most complete) version of each track.
    """
    tracks = {}

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            track_id = event.get("track_id")
            if not track_id:
                continue

            # Extract detections - each event has cumulative detections
            detections = event.get("detections", [])
            delays = [d["delay"] for d in detections]
            dopplers = [d["doppler"] for d in detections]
            snrs = [d["snr"] for d in detections]

            # Extract ADS-B expected delay/doppler where available
            adsb_delays = []
            adsb_dopplers = []
            for d in detections:
                a = d.get("adsb")
                if (
                    a
                    and a.get("expected_delay") is not None
                    and a.get("expected_doppler") is not None
                ):
                    adsb_delays.append(a["expected_delay"])
                    adsb_dopplers.append(a["expected_doppler"])

            # Keep latest (most complete) version of each track
            tracks[track_id] = {
                "delays": delays,
                "dopplers": dopplers,
                "snrs": snrs,
                "length": event.get("length", len(detections)),
                "adsb_hex": event.get("adsb_hex"),
                "adsb_delays": adsb_delays,
                "adsb_dopplers": adsb_dopplers,
            }

    return tracks


def plot_tracks(tracks, output_file, min_length=3):
    """Create delay-Doppler scatter plot of tracks."""
    fig, ax = plt.subplots(figsize=(14, 10))

    # Filter tracks by minimum length
    filtered = {
        tid: data for tid, data in tracks.items() if data["length"] >= min_length
    }

    # Color palette
    colors = plt.cm.tab20(np.linspace(0, 1, 20))

    for i, (track_id, data) in enumerate(filtered.items()):
        color = colors[i % 20]
        label = track_id
        if data["adsb_hex"]:
            label = f"{track_id} ({data['adsb_hex']})"

        ax.scatter(
            data["delays"],
            data["dopplers"],
            c=[color] * len(data["delays"]),
            s=20,
            alpha=0.7,
            edgecolors="none",
            label=label,
        )

        # ADS-B expected delay/doppler as square markers
        if data["adsb_delays"]:
            ax.scatter(
                data["adsb_delays"],
                data["adsb_dopplers"],
                c=[color] * len(data["adsb_delays"]),
                s=30,
                alpha=0.9,
                marker="s",
                edgecolors="black",
                linewidths=0.5,
            )

    # Styling
    ax.set_xlabel("Delay", fontsize=12)
    ax.set_ylabel("Doppler (Hz)", fontsize=12)
    ax.set_title("Tracker Output", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Legend
    n_tracks = len(filtered)
    if 0 < n_tracks <= 15:
        ax.legend(loc="lower left", fontsize=8, ncol=2)
    elif n_tracks > 15:
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles[:15],
            labels[:15],
            loc="lower left",
            fontsize=8,
            ncol=2,
            title=f"Showing 15 of {n_tracks} tracks",
        )

    plt.tight_layout()
    fig.savefig(output_file, dpi=150, bbox_inches="tight")
    print(f"Saved plot to {output_file}")


def main():
    from .utils import increment_filename

    parser = argparse.ArgumentParser(description="Plot tracks from JSONL output")
    parser.add_argument("input", help="Input JSONL file")
    parser.add_argument("-o", "--output", default="tracks.png", help="Output PNG file")
    parser.add_argument(
        "--min-length", type=int, default=3, help="Minimum track length to display"
    )
    args = parser.parse_args()
    args.output = increment_filename(args.output)

    tracks = load_tracks(args.input)
    print(f"Loaded {len(tracks)} tracks")
    plot_tracks(tracks, args.output, args.min_length)


if __name__ == "__main__":
    main()
