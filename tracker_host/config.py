"""Configuration loading and validation for tracker-host."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class ServerConfig:
    """HTTP server configuration for receiving track events from nodes."""

    host: str = "0.0.0.0"
    port: int = 8080
    geolocator_enabled: bool = False
    geolocator_tcp_port_base: int = 31000


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
class GeolocatorConfig:
    """Configuration for a geolocator instance paired with a tracker."""

    enabled: bool = False
    tcp_port: int = 0
    geolocator_config_path: Optional[str] = None


@dataclass
class TrackerConfig:
    """Configuration for a single tracker instance."""

    name: str
    tcp_port: int
    detection_url: Optional[str] = None
    mode: Optional[str] = None
    node_id: Optional[str] = None
    tracker_config: Optional[str] = None
    config_url: Optional[str] = None
    api_forward: Optional[ApiForwardConfig] = None
    geolocator: Optional[GeolocatorConfig] = None


@dataclass
class TcpServerConfig:
    """TCP push server configuration."""

    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 30050
    token: str = ""
    auto_provision: bool = True
    auto_provision_port_start: int = 31000
    auto_provision_port_end: int = 31099


@dataclass
class Config:
    """Main configuration for tracker-host."""

    output_dir: str = "./output"
    poll_interval_sec: float = 0.5
    status_interval_sec: float = 30.0
    startup_delay_sec: float = 1.0
    geolocator_startup_delay_sec: float = 2.0
    tcp_connect_retries: int = 10
    tcp_retry_delay_sec: float = 0.5
    config_poll_interval_sec: float = 60.0
    server: ServerConfig = field(default_factory=ServerConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    api_forward: ApiForwardConfig = field(default_factory=ApiForwardConfig)
    tcp_server: TcpServerConfig = field(default_factory=TcpServerConfig)
    trackers: list[TrackerConfig] = field(default_factory=list)
    retina_tracker_path: str = "../retina-tracker"
    retina_geolocator_path: str = "../retina-geolocator"


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
    server_raw = raw.get("server", {})
    server = ServerConfig(
        host=server_raw.get("host", "0.0.0.0"),
        port=server_raw.get("port", 8080),
        geolocator_enabled=server_raw.get("geolocator_enabled", False),
        geolocator_tcp_port_base=server_raw.get("geolocator_tcp_port_base", 31000),
    )

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

        detection_url = t.get("detection_url")
        explicit_mode = t.get("mode")

        if explicit_mode is not None:
            mode = explicit_mode
        elif detection_url is not None:
            mode = "http"
        else:
            mode = "tcp"

        tracker_geo = None
        if "geolocator" in t:
            tgeo = t["geolocator"]
            tracker_geo = GeolocatorConfig(
                enabled=tgeo.get("enabled", False),
                tcp_port=tgeo.get("tcp_port", 0),
                geolocator_config_path=tgeo.get("geolocator_config_path"),
            )

        trackers.append(
            TrackerConfig(
                name=t["name"],
                tcp_port=t["tcp_port"],
                detection_url=detection_url,
                mode=mode,
                node_id=t.get("node_id"),
                tracker_config=t.get("tracker_config"),
                config_url=t.get("config_url"),
                api_forward=tracker_api,
                geolocator=tracker_geo,
            )
        )

    tcp_server_raw = raw.get("tcp_server", {})
    tcp_server = TcpServerConfig(
        enabled=tcp_server_raw.get("enabled", False),
        host=tcp_server_raw.get("host", "0.0.0.0"),
        port=tcp_server_raw.get("port", 30050),
        token=tcp_server_raw.get("token", ""),
        auto_provision=tcp_server_raw.get("auto_provision", True),
        auto_provision_port_start=tcp_server_raw.get(
            "auto_provision_port_start", 31000
        ),
        auto_provision_port_end=tcp_server_raw.get("auto_provision_port_end", 31099),
    )

    return Config(
        server=server,
        output_dir=raw.get("output_dir", "./output"),
        poll_interval_sec=raw.get("poll_interval_sec", 0.5),
        status_interval_sec=raw.get("status_interval_sec", 30.0),
        startup_delay_sec=raw.get("startup_delay_sec", 1.0),
        geolocator_startup_delay_sec=raw.get("geolocator_startup_delay_sec", 2.0),
        tcp_connect_retries=raw.get("tcp_connect_retries", 10),
        tcp_retry_delay_sec=raw.get("tcp_retry_delay_sec", 0.5),
        config_poll_interval_sec=raw.get("config_poll_interval_sec", 60.0),
        retry=retry,
        api_forward=api_forward,
        tcp_server=tcp_server,
        trackers=trackers,
        retina_tracker_path=raw.get("retina_tracker_path", "../retina-tracker"),
        retina_geolocator_path=raw.get(
            "retina_geolocator_path", "../retina-geolocator"
        ),
    )
