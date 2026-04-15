from __future__ import annotations

from logwatch.collector.log_stream import LogStreamWatcher


def _make_watcher(host_config: dict) -> LogStreamWatcher:
    return LogStreamWatcher(
        host=host_config,
        stream_logs=None,
        prompt_template="default_alert",
        output_template="standard",
        config=host_config,
    )


class TestResolveContainerConfig:
    def test_no_overrides_returns_host_defaults(self) -> None:
        watcher = _make_watcher({"name": "prod"})
        prompt, output, config = watcher._resolve_container_config("api")
        assert prompt == "default_alert"
        assert output == "standard"

    def test_exact_match_overrides_templates(self) -> None:
        watcher = _make_watcher({
            "name": "prod",
            "containers": {
                "overrides": {
                    "api": {
                        "prompt_template": "api_alert",
                        "output_template": "detailed",
                    }
                }
            },
        })
        prompt, output, _config = watcher._resolve_container_config("api")
        assert prompt == "api_alert"
        assert output == "detailed"

    def test_wildcard_match(self) -> None:
        watcher = _make_watcher({
            "name": "prod",
            "containers": {
                "overrides": {
                    "worker-*": {
                        "prompt_template": "worker_alert",
                    }
                }
            },
        })
        prompt, output, _config = watcher._resolve_container_config("worker-heavy")
        assert prompt == "worker_alert"
        assert output == "standard"  # not overridden

    def test_no_match_returns_defaults(self) -> None:
        watcher = _make_watcher({
            "name": "prod",
            "containers": {
                "overrides": {
                    "api": {"prompt_template": "api_alert"},
                }
            },
        })
        prompt, output, _config = watcher._resolve_container_config("nginx")
        assert prompt == "default_alert"
        assert output == "standard"

    def test_rules_override_merges_with_host(self) -> None:
        watcher = _make_watcher({
            "name": "prod",
            "rules": {
                "ignore": ["healthcheck"],
                "alert_keywords": ["error", "fatal"],
            },
            "containers": {
                "overrides": {
                    "api": {
                        "rules": {
                            "alert_keywords": ["error", "5xx", "timeout"],
                            "ignore": ["metrics endpoint"],
                        }
                    }
                }
            },
        })
        _prompt, _output, config = watcher._resolve_container_config("api")
        rules = config.get("rules", {})
        # Container overrides replace, not append
        assert rules["alert_keywords"] == ["error", "5xx", "timeout"]
        assert rules["ignore"] == ["metrics endpoint"]

    def test_partial_rules_override_keeps_other_host_rules(self) -> None:
        watcher = _make_watcher({
            "name": "prod",
            "rules": {
                "ignore": ["healthcheck"],
                "alert_keywords": ["error", "fatal"],
                "redact": [{"pattern": "password=\\S+", "replace": "password=***"}],
            },
            "containers": {
                "overrides": {
                    "api": {
                        "rules": {
                            "alert_keywords": ["error", "5xx"],
                        }
                    }
                }
            },
        })
        _prompt, _output, config = watcher._resolve_container_config("api")
        rules = config.get("rules", {})
        assert rules["alert_keywords"] == ["error", "5xx"]
        assert rules["ignore"] == ["healthcheck"]  # kept from host
        assert len(rules["redact"]) == 1  # kept from host
