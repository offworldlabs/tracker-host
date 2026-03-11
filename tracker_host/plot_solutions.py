"""Plot geolocated solutions from tracker-host JSONL output."""

import argparse
import colorsys
import json
import math

import matplotlib.pyplot as plt
import numpy as np
import yaml


def normalize_longitude(lon):
    """Convert 0..360 longitude to -180..180."""
    if lon > 180:
        return lon - 360
    return lon


def load_solutions(filepath):
    """Load solution events from JSONL file.

    Returns dict: {track_id: [list of solution dicts]}
    Keeps all successful solutions per track for temporal progression.
    """
    solutions = {}

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            if not event.get("success", False):
                continue
            track_id = event.get("track_id")
            if not track_id:
                continue

            sol = {
                "latitude": event["latitude"],
                "longitude": normalize_longitude(event["longitude"]),
                "altitude": event.get("altitude"),
                "track_length": event.get("track_length"),
                "timestamp_start": event.get("timestamp_start"),
                "timestamp_end": event.get("timestamp_end"),
            }
            solutions.setdefault(track_id, []).append(sol)

    return solutions


def load_adsb_positions(filepath):
    """Load ADS-B lat/lon per track from tracks JSONL.

    Returns dict: {track_id: [(lat, lon), ...]}
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

            detections = event.get("detections", [])
            positions = []
            for d in detections:
                a = d.get("adsb")
                if a and a.get("lat") is not None and a.get("lon") is not None:
                    positions.append((a["lat"], a["lon"]))

            if positions:
                tracks[track_id] = positions

    return tracks


def load_radar_config(config_path):
    """Read .radar_config.yml, return RX/TX lat/lon/name."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    loc = cfg["location"]
    return {
        "rx": {
            "latitude": loc["rx"]["latitude"],
            "longitude": loc["rx"]["longitude"],
            "name": loc["rx"].get("name", "RX"),
        },
        "tx": {
            "latitude": loc["tx"]["latitude"],
            "longitude": loc["tx"]["longitude"],
            "name": loc["tx"].get("name", "TX"),
        },
    }


def track_colors(n, hue):
    """Generate light-to-dark gradient for one track.

    Returns list of n RGB tuples with lightness from 0.80 (early) to 0.30 (late).
    """
    if n == 1:
        return [colorsys.hls_to_rgb(hue, 0.50, 0.8)]
    colors = []
    for i in range(n):
        lightness = 0.80 - (0.50 * i / (n - 1))  # 0.80 -> 0.30
        colors.append(colorsys.hls_to_rgb(hue, lightness, 0.8))
    return colors


def km_per_degree_lon(lat):
    """Approximate km per degree of longitude at given latitude."""
    return 111.320 * math.cos(math.radians(lat))


def add_scale_bar(ax, lat_center):
    """Draw horizontal scale bar with km label at bottom-left."""
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    x_span = xlim[1] - xlim[0]

    km_per_deg = km_per_degree_lon(lat_center)
    total_km = x_span * km_per_deg

    # Pick a nice distance that fits ~20% of x-axis
    nice_distances = [1, 2, 5, 10, 20, 50, 100]
    target_km = total_km * 0.20
    bar_km = nice_distances[0]
    for d in nice_distances:
        if d <= target_km:
            bar_km = d
        else:
            break

    bar_deg = bar_km / km_per_deg

    # Position at bottom-left with some padding
    x0 = xlim[0] + x_span * 0.05
    y_span = ylim[1] - ylim[0]
    y0 = ylim[0] + y_span * 0.05

    ax.plot([x0, x0 + bar_deg], [y0, y0], color="black", linewidth=2, zorder=5)
    ax.text(
        x0 + bar_deg / 2,
        y0 + y_span * 0.02,
        f"{bar_km} km",
        ha="center",
        va="bottom",
        fontsize=8,
        zorder=5,
    )


def plot_solutions(
    solutions, output_file, radar_config=None, min_length=1, adsb_positions=None
):
    """Create lat/lon scatter plot of geolocated solutions."""
    fig, ax = plt.subplots(figsize=(14, 10))

    # Filter tracks by minimum number of solutions
    filtered = {tid: sols for tid, sols in solutions.items() if len(sols) >= min_length}

    n_tracks = len(filtered)
    total_solutions = sum(len(sols) for sols in filtered.values())

    for i, (track_id, sols) in enumerate(filtered.items()):
        hue = i / n_tracks if n_tracks > 0 else 0
        colors = track_colors(len(sols), hue)

        lons = [s["longitude"] for s in sols]
        lats = [s["latitude"] for s in sols]

        # Scatter points
        ax.scatter(
            lons,
            lats,
            c=colors,
            s=10,
            edgecolors="none",
            zorder=2,
        )

        # Connecting lines — mid-darkness color
        if len(sols) > 1:
            mid_color = colorsys.hls_to_rgb(hue, 0.50, 0.8)
            ax.plot(
                lons,
                lats,
                color=mid_color,
                linewidth=0.5,
                alpha=0.5,
                zorder=1,
            )

        # ADS-B position overlay as square markers
        if adsb_positions and track_id in adsb_positions:
            pts = adsb_positions[track_id]
            adsb_lons = [p[1] for p in pts]
            adsb_lats = [p[0] for p in pts]
            mid_color = colorsys.hls_to_rgb(hue, 0.50, 0.8)
            ax.scatter(
                adsb_lons,
                adsb_lats,
                c=[mid_color] * len(pts),
                s=30,
                alpha=0.9,
                marker="s",
                edgecolors="black",
                linewidths=0.5,
                zorder=2,
            )

    # RX marker + TX arrow
    if radar_config:
        rx = radar_config["rx"]
        tx = radar_config["tx"]
        ax.scatter(
            rx["longitude"],
            rx["latitude"],
            marker="^",
            c="red",
            s=80,
            zorder=3,
            label=f"RX: {rx['name']}",
        )

    # Compute center latitude for aspect ratio (exclude TX)
    all_lats = []
    for sols in filtered.values():
        all_lats.extend(s["latitude"] for s in sols)
    if radar_config:
        all_lats.append(rx["latitude"])

    if all_lats:
        lat_center = np.mean(all_lats)
        ax.set_aspect(1.0 / math.cos(math.radians(lat_center)))
    else:
        lat_center = 0

    # Add TX direction arrow after axis limits are established
    if radar_config:
        # Force matplotlib to compute axis limits from current data
        ax.autoscale_view()
        fig.canvas.draw()

        xlim = ax.get_xlim()
        ylim = ax.get_ylim()

        # Bearing and distance from RX to TX
        dlat = tx["latitude"] - rx["latitude"]
        dlon = tx["longitude"] - rx["longitude"]
        dist_lat_km = dlat * 111.32
        dist_lon_km = dlon * km_per_degree_lon(rx["latitude"])
        dist_km = math.sqrt(dist_lat_km**2 + dist_lon_km**2)
        angle = math.atan2(dlon, dlat)  # radians, 0=north, CW positive

        # Place arrow near edge of plot pointing toward TX
        cx = (xlim[0] + xlim[1]) / 2
        cy = (ylim[0] + ylim[1]) / 2
        hw = (xlim[1] - xlim[0]) / 2
        hh = (ylim[1] - ylim[0]) / 2

        # Unit direction in data coords
        dx = math.sin(angle)
        dy = math.cos(angle)

        # Scale to reach near edge from center
        if abs(dx) * hh > abs(dy) * hw and dx != 0:
            edge_scale = hw / abs(dx)
        elif dy != 0:
            edge_scale = hh / abs(dy)
        else:
            edge_scale = hw

        # Text at 70% toward edge, arrow tip at 95%
        text_x = cx + dx * edge_scale * 0.70
        text_y = cy + dy * edge_scale * 0.70
        tip_x = cx + dx * edge_scale * 0.95
        tip_y = cy + dy * edge_scale * 0.95

        # Choose alignment so text extends inward, not off-edge
        ha = "left" if dx < -0.3 else ("right" if dx > 0.3 else "center")
        va = "top" if dy > 0.3 else ("bottom" if dy < -0.3 else "center")

        tx_label = f"{tx['name']} ({dist_km:.1f} km)"
        ax.annotate(
            "",
            xy=(tip_x, tip_y),
            xytext=(text_x, text_y),
            arrowprops=dict(
                arrowstyle="->",
                color="blue",
                lw=2,
            ),
            zorder=4,
        )
        ax.text(
            text_x,
            text_y,
            tx_label,
            fontsize=9,
            fontweight="bold",
            color="blue",
            ha=ha,
            va=va,
            zorder=4,
        )
        ax.legend(loc="upper right", fontsize=9)

    # Grid and labels
    ax.set_xlabel("Longitude (\u00b0)", fontsize=12)
    ax.set_ylabel("Latitude (\u00b0)", fontsize=12)
    ax.set_title("Geolocated Solutions", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Track count + solution count annotation
    ax.text(
        0.02,
        0.98,
        f"{n_tracks} tracks, {total_solutions} solutions",
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment="top",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
    )

    # Scale bar
    if all_lats:
        add_scale_bar(ax, lat_center)

    plt.tight_layout()
    fig.savefig(output_file, dpi=150, bbox_inches="tight")
    print(f"Saved solution plot to {output_file}")


def main():
    from .utils import increment_filename

    parser = argparse.ArgumentParser(
        description="Plot geolocated solutions from JSONL output"
    )
    parser.add_argument("input", help="Input solutions JSONL file")
    parser.add_argument(
        "-o", "--output", default="solutions.png", help="Output PNG file"
    )
    parser.add_argument(
        "--radar-config", default=None, help="Path to .radar_config.yml"
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=1,
        help="Minimum number of solutions per track to display",
    )
    parser.add_argument(
        "--tracks", default=None, help="Tracks JSONL file for ADS-B overlay"
    )
    args = parser.parse_args()
    args.output = increment_filename(args.output)

    solutions = load_solutions(args.input)
    print(f"Loaded {len(solutions)} tracks with successful solutions")

    radar_config = None
    if args.radar_config:
        radar_config = load_radar_config(args.radar_config)

    adsb_positions = None
    if args.tracks:
        adsb_positions = load_adsb_positions(args.tracks)
        print(f"Loaded ADS-B positions for {len(adsb_positions)} tracks")

    plot_solutions(
        solutions, args.output, radar_config, args.min_length, adsb_positions
    )


if __name__ == "__main__":
    main()
