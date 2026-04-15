from __future__ import annotations

from logdog.pipeline.preprocessor.loader import load_preprocessors
from logdog.main import _resolve_host_preprocessor_configs


def test_resolve_host_preprocessor_configs_empty_host():
    assert _resolve_host_preprocessor_configs({}) == []


def test_resolve_host_preprocessor_configs_no_key():
    assert _resolve_host_preprocessor_configs({"url": "unix:///var/run/docker.sock"}) == []


def test_resolve_host_preprocessor_configs_returns_list():
    host = {
        "preprocessors": [
            {"name": "dedup", "max_consecutive": 3},
            {"name": "level_filter", "min_level": "warn"},
        ]
    }
    result = _resolve_host_preprocessor_configs(host)
    assert result == [
        {"name": "dedup", "max_consecutive": 3},
        {"name": "level_filter", "min_level": "warn"},
    ]


def test_resolve_host_preprocessor_configs_skips_non_dicts():
    host = {"preprocessors": [{"name": "dedup"}, "bad", None, 42]}
    assert _resolve_host_preprocessor_configs(host) == [{"name": "dedup"}]


def test_resolve_host_preprocessor_configs_non_list_returns_empty():
    assert _resolve_host_preprocessor_configs({"preprocessors": "dedup"}) == []


def test_load_preprocessors_with_host_dedup_config():
    host = {"preprocessors": [{"name": "dedup", "max_consecutive": 1}]}
    configs = _resolve_host_preprocessor_configs(host)
    chain = load_preprocessors(builtin_configs=configs)
    assert [p.name for p in chain] == ["default", "dedup"]
