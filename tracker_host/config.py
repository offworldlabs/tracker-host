"""Configuration loading and validation for tracker-host."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class RetryConfig:
    """Retry and resilience settings."""

    max_attempts: int = 5
    backoff_base_sec: float = 2.0
    extended_outage_sec: float = 60.0
    health_check_interval_sec: float = 30.0


@dataclass
class ApiForwardConfig:
    """API forwarding configuration."""

    enabled: bool = False
    url: str = ""
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class TrackerConfig:
    """Configuration for a single tracker instance."""

    name: str
    detection_url: str
    tcp_port: int
    tcp_host: str = "127.0.0.1"
    spawn_tracker: bool = True
    tracker_config: Optional[str] = None
    api_forward: Optional[ApiForwardConfig] = None


@dataclass
class Config:
    """Main configuration for tracker-host."""

    output_dir: str = "./output"
    poll_interval_sec: float = 0.5
    status_interval_sec: float = 30.0
    retry: RetryConfig = field(default_factory=RetryConfig)
    api_forward: ApiForwardConfig = field(default_factory=ApiForwardConfig)
    trackers: list[TrackerConfig] = field(default_factory=list)
    retina_tracker_path: str = "../retina-tracker"


def load_config(config_path: str | Path) -> Config:
    """Load configuration from a YAML file."""
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    return _parse_config(raw)


def _parse_config(raw: dict) -> Config:
    """Parse raw YAML dict into Config dataclass."""
    retry_raw = raw.get("retry", {})
    retry = RetryConfig(
        max_attempts=retry_raw.get("max_attempts", 5),
        backoff_base_sec=retry_raw.get("backoff_base_sec", 2.0),
        extended_outage_sec=retry_raw.get("extended_outage_sec", 60.0),
        health_check_interval_sec=retry_raw.get("health_check_interval_sec", 30.0),
    )

    api_forward_raw = raw.get("api_forward", {})
    api_forward = ApiForwardConfig(
        enabled=api_forward_raw.get("enabled", False),
        url=api_forward_raw.get("url", ""),
        headers=api_forward_raw.get("headers", {}),
    )

    trackers = []
    for t in raw.get("trackers", []):
        tracker_api = None
        if "api_forward" in t:
            tapi = t["api_forward"]
            tracker_api = ApiForwardConfig(
                enabled=tapi.get("enabled", False),
                url=tapi.get("url", ""),
                headers=tapi.get("headers", {}),
            )

        trackers.append(
            TrackerConfig(
                name=t["name"],
                detection_url=t["detection_url"],
                tcp_port=t["tcp_port"],
                tcp_host=t.get("tcp_host", "127.0.0.1"),
                spawn_tracker=t.get("spawn_tracker", True),
                tracker_config=t.get("tracker_config"),
                api_forward=tracker_api,
            )
        )

    return Config(
        output_dir=raw.get("output_dir", "./output"),
        poll_interval_sec=raw.get("poll_interval_sec", 0.5),
        status_interval_sec=raw.get("status_interval_sec", 30.0),
        retry=retry,
        api_forward=api_forward,
        trackers=trackers,
        retina_tracker_path=raw.get("retina_tracker_path", "../retina-tracker"),
    )
