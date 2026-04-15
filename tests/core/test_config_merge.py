from copy import deepcopy
import pytest

from logdog.core.config import (
    expand_effective_hosts,
    merge_host_config,
    resolve_runtime_settings,
)


def test_merge_with_replace_schedule_mode():
    merged = merge_host_config(
        defaults={
            "schedules_mode": "append",
            "schedules": [{"name": "five", "interval_seconds": 300}],
        },
        host={
            "schedules_mode": "replace",
            "schedules": [{"name": "fast", "interval_seconds": 60}],
        },
    )

    assert merged["schedules_mode"] == "replace"
    assert [x["name"] for x in merged["schedules"]] == ["fast"]


def test_schedules_append_and_host_override():
    merged = merge_host_config(
        defaults={
            "schedules": [
                {"name": "single", "interval_seconds": 100},
                {"name": "common", "interval_seconds": 200},
            ],
        },
        host={
            "schedules": [
                {"name": "host", "interval_seconds": 10},
                {"name": "common", "interval_seconds": 30},
            ],
        },
    )

    assert [x["name"] for x in merged["schedules"]] == ["single", "host", "common"]
    common_entry = next(x for x in merged["schedules"] if x["name"] == "common")
    assert common_entry["interval_seconds"] == 30


def test_schedules_dedup_last_occurrence():
    merged = merge_host_config(
        defaults={
            "schedules": [
                {"name": "alpha", "interval_seconds": 10},
                {"name": "alpha", "interval_seconds": 20},
                {"name": "beta", "interval_seconds": 30},
            ]
        },
        host={
            "schedules": [
                {"name": "beta", "interval_seconds": 40},
                {"name": "gamma", "interval_seconds": 50},
                {"name": "beta", "interval_seconds": 60},
            ]
        },
    )

    assert [x["name"] for x in merged["schedules"]] == ["alpha", "gamma", "beta"]
    assert merged["schedules"][0]["interval_seconds"] == 20
    assert merged["schedules"][2]["interval_seconds"] == 60


def test_containers_merge_include_replace_exclude_append():
    merged = merge_host_config(
        defaults={
            "containers": {
                "include": ["default-include"],
                "exclude": ["default-exclude"],
            }
        },
        host={
            "containers": {
                "include": ["host-include"],
                "exclude": ["host-exclude"],
            }
        },
    )

    assert merged["containers"]["include"] == ["host-include"]
    assert merged["containers"]["exclude"] == ["default-exclude", "host-exclude"]


def test_rules_merge_append_and_replace_modes():
    merged = merge_host_config(
        defaults={
            "rules": {
                "ignore": ["default-ignore"],
                "redact": ["default-redact"],
                "custom_alerts": ["default-alert"],
            }
        },
        host={
            "rules": {
                "ignore": ["host-ignore"],
                "ignore_mode": "replace",
                "redact": ["host-redact"],
                "custom_alerts": ["host-alert"],
            }
        },
    )

    assert merged["rules"]["ignore"] == ["host-ignore"]
    assert merged["rules"]["redact"] == ["default-redact", "host-redact"]
    assert merged["rules"]["custom_alerts"] == ["default-alert", "host-alert"]


def test_rules_pattern_dedup_prefers_host():
    merged = merge_host_config(
        defaults={
            "rules": {
                "redact": [
                    {"pattern": "secret", "mask": "x"},
                    {"pattern": "public"},
                ]
            }
        },
        host={
            "rules": {
                "redact": [{"pattern": "secret", "mask": "y"}],
            }
        },
    )

    assert len(merged["rules"]["redact"]) == 2
    assert merged["rules"]["redact"][0]["mask"] == "y"


def test_notify_deep_merge_preserves_defaults():
    defaults = {
        "notify": {
            "telegram": {"chat_ids": [1111], "enabled": True},
            "wechat": {"webhook_urls": ["default-webhook"], "enabled": True},
        }
    }
    host = {
        "notify": {
            "telegram": {"enabled": False},
            "wechat": {"webhook_urls": ["host-webhook"], "enabled": False},
        }
    }

    merged = merge_host_config(defaults, host)

    assert merged["notify"]["telegram"]["chat_ids"] == [1111]
    assert merged["notify"]["telegram"]["enabled"] is False
    assert merged["notify"]["wechat"]["webhook_urls"] == ["host-webhook"]
    assert merged["notify"]["wechat"]["enabled"] is False


def test_merge_does_not_mutate_inputs():
    defaults = {
        "rules": {"ignore": ["default-ignore"]},
        "notify": {"telegram": {"chat_ids": [1]}},
        "schedules": [{"name": "base", "interval_seconds": 10}],
    }
    host = {
        "rules": {"ignore": ["host-ignore"]},
        "notify": {"telegram": {"chat_ids": [2]}},
    }

    defaults_snapshot = deepcopy(defaults)
    host_snapshot = deepcopy(host)

    merge_host_config(defaults, host)

    assert defaults == defaults_snapshot
    assert host == host_snapshot


def test_rules_invalid_list_type_raises():
    with pytest.raises(TypeError):
        merge_host_config({"rules": {}}, {"rules": {"ignore": "wrong"}})


def test_invalid_schedules_mode_raises():
    with pytest.raises(ValueError):
        merge_host_config({}, {"schedules_mode": "invalid"})


def test_invalid_schedules_mode_type_error():
    with pytest.raises(TypeError):
        merge_host_config({}, {"schedules_mode": 123})


def test_invalid_rules_mode_raises():
    with pytest.raises(ValueError):
        merge_host_config({"rules": {}}, {"rules": {"ignore_mode": "invalid"}})


def test_invalid_rules_mode_type_error():
    with pytest.raises(TypeError):
        merge_host_config({"rules": {}}, {"rules": {"ignore_mode": 123}})


def test_defaults_notify_channel_must_be_dict():
    with pytest.raises(TypeError):
        merge_host_config({"notify": {"telegram": "broken"}}, {})


def test_defaults_notify_list_field_validation():
    with pytest.raises(TypeError):
        merge_host_config({"notify": {"telegram": {"chat_ids": "bad"}}}, {})


def test_notify_list_fields_type_validation():
    with pytest.raises(TypeError):
        merge_host_config({}, {"notify": {"telegram": {"chat_ids": "bad"}}})


def test_schedule_item_must_be_dict():
    with pytest.raises(TypeError):
        merge_host_config({"schedules": ["bad"]}, {})


def test_notify_chat_and_webhook_replace():
    merged = merge_host_config(
        defaults={
            "notify": {
                "telegram": {"chat_ids": [1111]},
                "wechat": {"webhook_urls": ["default-webhook"]},
            }
        },
        host={
            "notify": {
                "telegram": {"chat_ids": [2222]},
                "wechat": {"webhook_urls": ["host-webhook"]},
            }
        },
    )

    assert merged["notify"]["telegram"]["chat_ids"] == [2222]
    assert merged["notify"]["wechat"]["webhook_urls"] == ["host-webhook"]


def test_notify_channel_alias_list_replace() -> None:
    merged = merge_host_config(
        defaults={
            "notify": {
                "channels": ["tel1", "wx1"],
            }
        },
        host={
            "notify": {
                "channels": ["tel2"],
            }
        },
    )

    assert merged["notify"]["channels"] == ["tel2"]


def test_notify_channel_alias_list_must_be_list() -> None:
    with pytest.raises(TypeError):
        merge_host_config({}, {"notify": {"channels": "tel1"}})


def test_resolve_runtime_settings_defaults():
    out = resolve_runtime_settings(None)
    assert out["metrics_interval_seconds"] == 30
    assert out["retention_interval_seconds"] == 86400
    assert out["metrics_db_path"] is None
    assert out["retention_config"] == {}
    assert out["host_system"] == {
        "enabled": False,
        "sample_interval_seconds": 30,
        "collect_load": True,
        "collect_network": True,
        "security": {
            "enabled": False,
            "push_on_issue": True,
        },
        "report": {
            "include_in_schedule": True,
            "warn_thresholds": {
                "cpu_percent": 85.0,
                "mem_used_percent": 90.0,
                "disk_used_percent": 90.0,
            },
        },
    }
    assert out["sqlite"] == {
        "journal_mode": "wal",
        "synchronous": "normal",
        "busy_timeout_ms": 5000,
    }


def test_resolve_runtime_settings_parses_sqlite_tuning() -> None:
    out = resolve_runtime_settings(
        {
            "storage": {
                "sqlite": {
                    "journal_mode": "wal",
                    "synchronous": "normal",
                    "busy_timeout_ms": 5000,
                }
            }
        }
    )
    assert out["sqlite"]["journal_mode"] == "wal"
    assert out["sqlite"]["synchronous"] == "normal"
    assert out["sqlite"]["busy_timeout_ms"] == 5000


def test_resolve_runtime_settings_parses_host_system_config() -> None:
    out = resolve_runtime_settings(
        {
            "metrics": {
                "host_system": {
                    "enabled": True,
                    "sample_interval_seconds": "45",
                    "collect_load": False,
                    "collect_network": True,
                    "security": {
                        "enabled": True,
                        "push_on_issue": False,
                    },
                    "report": {
                        "include_in_schedule": False,
                        "warn_thresholds": {
                            "cpu_percent": "91.5",
                            "mem_used_percent": 92,
                            "disk_used_percent": "93",
                        },
                    },
                }
            }
        }
    )

    assert out["host_system"] == {
        "enabled": True,
        "sample_interval_seconds": 45,
        "collect_load": False,
        "collect_network": True,
        "security": {
            "enabled": True,
            "push_on_issue": False,
        },
        "report": {
            "include_in_schedule": False,
            "warn_thresholds": {
                "cpu_percent": pytest.approx(91.5),
                "mem_used_percent": pytest.approx(92.0),
                "disk_used_percent": pytest.approx(93.0),
            },
        },
    }


def test_resolve_runtime_settings_host_system_threshold_out_of_range_raises() -> None:
    with pytest.raises(ValueError, match="cpu_percent"):
        resolve_runtime_settings(
            {
                "metrics": {
                    "host_system": {
                        "enabled": True,
                        "report": {"warn_thresholds": {"cpu_percent": 101}},
                    }
                }
            }
        )


def test_resolve_runtime_settings_host_system_bool_fields_must_be_bool() -> None:
    with pytest.raises(TypeError, match="host_system.enabled"):
        resolve_runtime_settings(
            {
                "metrics": {
                    "host_system": {
                        "enabled": "true",
                    }
                }
            }
        )


def test_resolve_runtime_settings_host_system_security_bool_fields_must_be_bool() -> (
    None
):
    with pytest.raises(TypeError, match="host_system.security.enabled"):
        resolve_runtime_settings(
            {
                "metrics": {
                    "host_system": {
                        "security": {
                            "enabled": "true",
                        }
                    }
                }
            }
        )


def test_resolve_runtime_settings_negative_retention_raises():
    with pytest.raises(ValueError):
        resolve_runtime_settings(
            {
                "storage": {
                    "retention": {
                        "alerts_days": -1,
                    }
                }
            }
        )


def test_resolve_runtime_settings_parses_docker_pool_limits():
    out = resolve_runtime_settings(
        {
            "docker": {
                "pool": {
                    "max_clients": "3",
                    "max_idle_seconds": "60",
                }
            }
        }
    )
    assert out["docker_pool_max_clients"] == 3
    assert out["docker_pool_max_idle_seconds"] == pytest.approx(60.0)


def test_resolve_runtime_settings_parses_scheduler_guardrails() -> None:
    out = resolve_runtime_settings(
        {
            "scheduler": {
                "max_instances": 1,
                "coalesce": True,
                "misfire_grace_time": 30,
            }
        }
    )
    assert out["scheduler"] == {
        "max_instances": 1,
        "coalesce": True,
        "misfire_grace_time": 30,
    }


@pytest.mark.parametrize(
    ("config", "expected_message"),
    [
        ({"metrics": {"sample_interval_seconds": True}}, "sample_interval_seconds"),
        ({"docker": {"pool": {"max_clients": False}}}, "max_clients"),
    ],
)
def test_resolve_runtime_settings_rejects_boolean_numeric_values(
    config: dict, expected_message: str
) -> None:
    with pytest.raises(TypeError, match=expected_message):
        resolve_runtime_settings(config)


@pytest.mark.parametrize(
    ("config", "expected_message"),
    [
        ({"metrics": {"sample_interval_seconds": 1.5}}, "sample_interval_seconds"),
        ({"docker": {"pool": {"max_clients": "3.2"}}}, "max_clients"),
    ],
)
def test_resolve_runtime_settings_rejects_non_integral_numeric_values(
    config: dict, expected_message: str
) -> None:
    with pytest.raises(TypeError, match=expected_message):
        resolve_runtime_settings(config)


def test_expand_effective_hosts_merges_defaults_into_each_host() -> None:
    expanded = expand_effective_hosts(
        {
            "defaults": {
                "ssh_key": "/keys/default.pem",
                "containers": {"exclude": ["infra"]},
                "notify": {"telegram": {"chat_ids": [1111], "enabled": True}},
            },
            "hosts": [
                {
                    "name": "prod",
                    "url": "ssh://deploy@10.0.1.10",
                    "containers": {"include": ["api"]},
                },
                {
                    "name": "worker",
                    "url": "unix:///var/run/docker.sock",
                    "notify": {"telegram": {"enabled": False}},
                },
            ],
        }
    )

    assert expanded[0]["name"] == "prod"
    assert expanded[0]["ssh_key"] == "/keys/default.pem"
    assert expanded[0]["containers"] == {"include": ["api"], "exclude": ["infra"]}
    assert expanded[0]["notify"]["telegram"]["chat_ids"] == [1111]
    assert expanded[1]["notify"]["telegram"]["enabled"] is False
    assert expanded[1]["notify"]["telegram"]["chat_ids"] == [1111]


def test_expand_effective_hosts_requires_hosts_list() -> None:
    with pytest.raises(TypeError, match="hosts"):
        expand_effective_hosts({"hosts": {"name": "broken"}})


def test_expand_effective_hosts_rejects_duplicate_host_names() -> None:
    with pytest.raises(ValueError, match="duplicate host name"):
        expand_effective_hosts(
            {
                "hosts": [
                    {"name": "dup", "url": "unix:///var/run/docker.sock"},
                    {"name": "dup", "url": "ssh://ops@10.0.0.2"},
                ]
            }
        )


def test_merge_containers_preserves_overrides() -> None:
    defaults = {
        "containers": {
            "exclude": ["logdog"],
        }
    }
    host = {
        "name": "prod",
        "containers": {
            "include": ["api", "worker"],
            "overrides": {
                "api": {"prompt_template": "api_alert"},
            },
        },
    }
    merged = merge_host_config(defaults, host)
    containers = merged["containers"]
    assert containers["include"] == ["api", "worker"]
    assert containers["exclude"] == ["logdog"]
    assert containers["overrides"]["api"]["prompt_template"] == "api_alert"
