import pytest

from logdog.pipeline.filter import apply_rules


def sample_config():
    return {
        "rules": {
            "ignore": [{"pattern": r"IGNOREME"}],
            "redact": [{"pattern": r"Bearer\s+\S+", "replace": "Bearer ***REDACTED***"}],
            "custom_alerts": [
                {"pattern": r"资金不足", "category": "BUSINESS", "severity": "critical"},
            ],
        },
        "alert_keywords": ["ERROR", "FATAL", "panic", "OOM", "timeout", "failed"],
    }


def test_ignore_has_priority():
    cfg = sample_config()
    line = "IGNOREME Bearer abc ERROR"
    result = apply_rules(line, config=cfg)
    assert result.triggered is False
    # ignore short-circuits: no redact
    assert result.redacted_line == line
    assert result.matched_category is None


def test_redact_applies_before_alert_checks():
    cfg = sample_config()
    line = "Bearer abc ERROR"
    result = apply_rules(line, config=cfg)
    assert result.triggered is True
    assert result.redacted_line == "Bearer ***REDACTED*** ERROR"
    assert result.matched_category == "ERROR"


def test_custom_alerts_hit_before_system_keywords():
    cfg = sample_config()
    line = "资金不足 ERROR"
    result = apply_rules(line, config=cfg)
    assert result.triggered is True
    assert result.matched_category == "BUSINESS"


def test_system_keyword_fallback_hit():
    cfg = sample_config()
    line = "fatal: something crashed"
    result = apply_rules(line, config=cfg)
    assert result.triggered is True
    assert result.matched_category == "ERROR"


def test_alert_keywords_under_rules_are_used_for_matching():
    cfg = sample_config()
    cfg.pop("alert_keywords")
    cfg["rules"]["alert_keywords"] = ["5xx"]
    result = apply_rules("5xx upstream broken", config=cfg)
    assert result.triggered is True
    assert result.matched_category == "ERROR"


def test_alert_keywords_must_not_be_str():
    cfg = sample_config()
    cfg["alert_keywords"] = "ERROR"
    with pytest.raises(TypeError):
        apply_rules("ERROR", config=cfg)


def test_alert_keywords_items_must_be_str():
    cfg = sample_config()
    cfg["alert_keywords"] = ["ERROR", 123]
    with pytest.raises(TypeError):
        apply_rules("ERROR", config=cfg)


def test_custom_alerts_missing_category_defaults_to_business():
    cfg = sample_config()
    cfg["rules"]["custom_alerts"] = [{"pattern": r"资金不足"}]
    result = apply_rules("资金不足", config=cfg)
    assert result.triggered is True
    assert result.matched_category == "BUSINESS"


def test_invalid_overlong_regex_pattern_rejected():
    cfg = sample_config()
    cfg["rules"]["ignore"] = [{"pattern": "a" * 2000}]
    with pytest.raises(ValueError):
        apply_rules("plain text", config=cfg)
