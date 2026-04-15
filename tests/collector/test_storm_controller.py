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
