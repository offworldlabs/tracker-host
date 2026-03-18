"""Entry point for tracker-host: python -m tracker_host"""

import argparse
import asyncio
import sys
from pathlib import Path

from .manager import run_manager


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="tracker-host: Manages multiple retina-tracker instances",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m tracker_host                    # Use default config.yaml
  python -m tracker_host -c myconfig.yaml   # Use custom config
  python -m tracker_host -v                 # Verbose logging
  python -m tracker_host --server           # Run in server mode
  python -m tracker_host --server -v        # Server mode with verbose logging
        """,
    )

    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    parser.add_argument(
        "--server",
        action="store_true",
        help="Run in server mode (receive tracks from nodes via HTTP)",
    )

    args = parser.parse_args()

    # Check config exists
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        print(
            "Create a config.yaml or specify a different path with -c", file=sys.stderr
        )
        sys.exit(1)

    # Run the appropriate mode
    try:
        if args.server:
            from .server_mode import run_server
            asyncio.run(run_server(str(config_path), verbose=args.verbose))
        else:
            asyncio.run(run_manager(str(config_path), verbose=args.verbose))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
