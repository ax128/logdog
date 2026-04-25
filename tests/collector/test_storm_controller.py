from __future__ import annotations

import pytest

from logdog.collector.storm import AlertStormController


def test_storm_controller_rejects_invalid_positive_parameters_when_enabled() -> None:
    with pytest.raises(ValueError, match="window_seconds"):
        AlertStormController(
            enabled=True,
            window_seconds=0,
            threshold=3,
            suppress_minutes=1,
        )

    with pytest.raises(ValueError, match="threshold"):
        AlertStormController(
            enabled=True,
            window_seconds=60,
            threshold=0,
            suppress_minutes=1,
        )

    with pytest.raises(ValueError, match="suppress_minutes"):
        AlertStormController(
            enabled=True,
            window_seconds=60,
            threshold=3,
            suppress_minutes=0,
        )


def test_storm_controller_allows_non_positive_parameters_when_disabled() -> None:
    controller = AlertStormController(
        enabled=False,
        window_seconds=0,
        threshold=0,
        suppress_minutes=0,
    )

    assert controller.enabled is False


def test_disabled_controller_always_returns_normal() -> None:
    ctrl = AlertStormController(
        enabled=False, window_seconds=0, threshold=0, suppress_minutes=0,
    )
    result = ctrl.record_event(host="h1", category="ERROR", line="oops", now=100.0)
    assert result["mode"] == "normal"


def test_events_below_threshold_return_normal() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=5, suppress_minutes=1.0,
    )
    for i in range(4):
        result = ctrl.record_event(
            host="h1", category="ERROR", line=f"line-{i}", now=100.0 + i,
        )
        assert result["mode"] == "normal"


def test_reaching_threshold_triggers_storm_start() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=3, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="line-0", now=100.0)
    ctrl.record_event(host="h2", category="ERROR", line="line-1", now=101.0)
    result = ctrl.record_event(host="h1", category="ERROR", line="line-2", now=102.0)
    assert result["mode"] == "storm_start"
    assert result["category"] == "STORM"
    assert result["payload"]["storm_total"] == 3
    assert sorted(result["payload"]["storm_hosts"]) == ["h1", "h2"]


def test_events_during_storm_are_suppressed() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=2, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="line-0", now=100.0)
    ctrl.record_event(host="h1", category="ERROR", line="line-1", now=101.0)
    result = ctrl.record_event(host="h2", category="ERROR", line="line-2", now=102.0)
    assert result["mode"] == "suppressed"


def test_different_categories_independent() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=2, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="e-0", now=100.0)
    result_error = ctrl.record_event(host="h1", category="ERROR", line="e-1", now=101.0)
    assert result_error["mode"] == "storm_start"

    result_warn = ctrl.record_event(host="h1", category="WARN", line="w-0", now=102.0)
    assert result_warn["mode"] == "normal"


def test_flush_due_returns_expired_storms() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=2, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="line-0", now=100.0)
    ctrl.record_event(host="h1", category="ERROR", line="line-1", now=101.0)

    assert ctrl.flush_due(now=160.0) == []

    ended = ctrl.flush_due(now=162.0)
    assert len(ended) == 1
    assert ended[0]["category"] == "STORM_END"
    assert ended[0]["payload"]["storm_phase"] == "end"
    assert ended[0]["payload"]["category"] == "ERROR"


def test_flush_due_on_disabled_returns_empty() -> None:
    ctrl = AlertStormController(
        enabled=False, window_seconds=0, threshold=0, suppress_minutes=0,
    )
    assert ctrl.flush_due(now=100.0) == []


def test_storm_session_tracks_total_count_across_suppressed_events() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=2, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="line-0", now=100.0)
    ctrl.record_event(host="h1", category="ERROR", line="line-1", now=101.0)
    for i in range(3):
        ctrl.record_event(host="h1", category="ERROR", line=f"sup-{i}", now=102.0 + i)

    ended = ctrl.flush_due(now=200.0)
    assert len(ended) == 1
    assert ended[0]["payload"]["storm_total"] == 5


def test_old_events_outside_window_pruned() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=10, threshold=3, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="old-0", now=100.0)
    ctrl.record_event(host="h1", category="ERROR", line="old-1", now=101.0)
    ctrl.record_event(host="h1", category="ERROR", line="new-0", now=115.0)
    result = ctrl.record_event(host="h1", category="ERROR", line="new-1", now=116.0)
    assert result["mode"] == "normal"


def test_reset_clears_all_state() -> None:
    ctrl = AlertStormController(
        enabled=True, window_seconds=60, threshold=2, suppress_minutes=1.0,
    )
    ctrl.record_event(host="h1", category="ERROR", line="line-0", now=100.0)
    ctrl.record_event(host="h1", category="ERROR", line="line-1", now=101.0)
    ctrl.reset()
    result = ctrl.record_event(host="h1", category="ERROR", line="line-2", now=102.0)
    assert result["mode"] == "normal"
