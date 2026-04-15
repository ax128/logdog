from __future__ import annotations

from dataclasses import dataclass, field


def _minutes_to_seconds(minutes: float) -> float:
    if minutes < 0:
        raise ValueError("cooldown minutes must be >= 0")
    return minutes * 60.0


@dataclass(slots=True)
class CooldownStore:
    """
    In-memory cooldown window store.

    Keyed by (host, container_id, category).
    'now' is a monotonic/epoch seconds number supplied by caller (for testability).
    """

    default_minutes: float = 30.0
    per_category: dict[str, float] = field(default_factory=dict)
    stale_after_seconds: float = 24 * 3600.0
    max_entries: int = 10_000
    prune_interval_seconds: float = 60.0
    _last_allowed_at: dict[tuple[str, str, str], float] = field(
        default_factory=dict, init=False
    )
    _last_prune_at: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        if self.max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        if self.prune_interval_seconds <= 0:
            raise ValueError("prune_interval_seconds must be > 0")
        if self.stale_after_seconds < 0:
            raise ValueError("stale_after_seconds must be >= 0")

    def window_seconds(self, category: str) -> float:
        minutes = self.per_category.get(category, self.default_minutes)
        return _minutes_to_seconds(float(minutes))

    def allow(self, host: str, container_id: str, category: str, now: float) -> bool:
        now = float(now)
        self._prune(now)
        key = (host, container_id, category)
        window = self.window_seconds(category)
        last = self._last_allowed_at.get(key)
        if last is None or now < last or (now - last) >= window:
            self._last_allowed_at[key] = float(now)
            return True
        return False

    def last_allowed_at(
        self, host: str, container_id: str, category: str
    ) -> float | None:
        return self._last_allowed_at.get((host, container_id, category))

    def reset(self) -> None:
        self._last_allowed_at.clear()
        self._last_prune_at = 0.0

    def _prune(self, now: float) -> None:
        should_prune = len(self._last_allowed_at) > self.max_entries
        if (
            not should_prune
            and (now - self._last_prune_at) < self.prune_interval_seconds
        ):
            return

        if self.stale_after_seconds > 0:
            cutoff = now - self.stale_after_seconds
            stale_keys = [
                key for key, ts in self._last_allowed_at.items() if ts < cutoff
            ]
            for key in stale_keys:
                self._last_allowed_at.pop(key, None)

        if len(self._last_allowed_at) > self.max_entries:
            ordered = sorted(self._last_allowed_at.items(), key=lambda item: item[1])
            overflow = len(self._last_allowed_at) - self.max_entries
            for idx in range(overflow):
                self._last_allowed_at.pop(ordered[idx][0], None)

        self._last_prune_at = now
