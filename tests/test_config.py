"""Tests for tracker_host.config."""

import yaml

from tracker_host.config import Config, load_config, _parse_config


class TestParseConfig:
    def test_defaults(self):
        config = _parse_config({})
        assert config.output_dir == "./output"
        assert config.poll_interval_sec == 0.5
        assert config.startup_delay_sec == 1.0
        assert config.geolocator_startup_delay_sec == 2.0
        assert config.tcp_connect_retries == 10
        assert config.tcp_retry_delay_sec == 0.5
        assert config.config_poll_interval_sec == 60.0
        assert config.retina_tracker_path == "../retina-tracker"
        assert config.retina_geolocator_path == "../retina-geolocator"

    def test_custom_values(self):
        raw = {
            "output_dir": "/tmp/test",
            "startup_delay_sec": 3.0,
            "tcp_connect_retries": 5,
            "tcp_retry_delay_sec": 1.0,
            "config_poll_interval_sec": 120.0,
            "geolocator_startup_delay_sec": 4.0,
        }
        config = _parse_config(raw)
        assert config.output_dir == "/tmp/test"
        assert config.startup_delay_sec == 3.0
        assert config.tcp_connect_retries == 5
        assert config.tcp_retry_delay_sec == 1.0
        assert config.config_poll_interval_sec == 120.0
        assert config.geolocator_startup_delay_sec == 4.0

    def test_tracker_config(self):
        raw = {
            "trackers": [
                {
                    "name": "radar1",
                    "detection_url": "http://localhost:8080/detections",
                    "tcp_port": 5000,
                }
            ]
        }
        config = _parse_config(raw)
        assert len(config.trackers) == 1
        assert config.trackers[0].name == "radar1"
        assert config.trackers[0].tcp_port == 5000

    def test_retry_config(self):
        raw = {"retry": {"max_attempts": 10, "backoff_base_sec": 5.0}}
        config = _parse_config(raw)
        assert config.retry.max_attempts == 10
        assert config.retry.backoff_base_sec == 5.0

    def test_load_config_from_file(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            yaml.dump(
                {
                    "output_dir": "/data/output",
                    "poll_interval_sec": 1.0,
                    "startup_delay_sec": 2.5,
                    "trackers": [
                        {
                            "name": "test",
                            "detection_url": "http://test/api",
                            "tcp_port": 9000,
                        }
                    ],
                }
            )
        )
        config = load_config(str(config_file))
        assert config.output_dir == "/data/output"
        assert config.poll_interval_sec == 1.0
        assert config.startup_delay_sec == 2.5
        assert len(config.trackers) == 1
