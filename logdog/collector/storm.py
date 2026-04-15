from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class _StormSession:
    category: str
    started_at: float
    suppress_until: float
    total_count: int
    hosts: set[str] = field(default_factory=set)
    sample_lines: list[str] = field(default_factory=list)
    notify_host: str = ""


class AlertStormController:
    def __init__(
        self,
        *,
        enabled: bool,
        window_seconds: int,
        threshold: int,
        suppress_minutes: float,
    ) -> None:
        self.enabled = bool(enabled)
        self.window_seconds = int(window_seconds)
        self.threshold = int(threshold)
        self.suppress_minutes = float(suppress_minutes)
        if self.enabled:
            if self.window_seconds <= 0:
                raise ValueError(
                    "window_seconds must be > 0 when storm control enabled"
                )
            if self.threshold <= 0:
                raise ValueError("threshold must be > 0 when storm control enabled")
            if self.suppress_minutes <= 0:
                raise ValueError(
                    "suppress_minutes must be > 0 when storm control enabled"
                )
        self._recent_by_category: dict[str, list[dict[str, Any]]] = {}
        self._active_by_category: dict[str, _StormSession] = {}

    def flush_due(self, now: float) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        out: list[dict[str, Any]] = []
        expired = [
            category
            for category, session in self._active_by_category.items()
            if float(now) >= float(session.suppress_until)
        ]
        for category in expired:
            session = self._active_by_category.pop(category)
            out.append(
                {
                    "host": session.notify_host,
                    "category": "STORM_END",
                    "message": f"风暴结束，共 {session.total_count} 条告警，涉及 {len(session.hosts)} 台主机",
                    "payload": {
                        "storm_summary": True,
                        "storm_phase": "end",
                        "category": category,
                        "storm_total": session.total_count,
                        "storm_hosts": sorted(session.hosts),
                    },
                }
            )
        return out

    def record_event(
        self,
        *,
        host: str,
        category: str,
        line: str,
        now: float,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"mode": "normal"}

        active = self._active_by_category.get(category)
        if active is not None and float(now) < float(active.suppress_until):
            active.total_count += 1
            active.hosts.add(host)
            if len(active.sample_lines) < 3:
                active.sample_lines.append(line)
            return {"mode": "suppressed"}

        cutoff = float(now) - float(self.window_seconds)
        recent = [
            item
            for item in self._recent_by_category.get(category, [])
            if float(item["ts"]) >= cutoff
        ]
        recent.append({"ts": float(now), "host": host, "line": line})
        self._recent_by_category[category] = recent

        if len(recent) < self.threshold:
            return {"mode": "normal"}

        hosts = {str(item["host"]) for item in recent}
        sample_lines = [str(item["line"]) for item in recent[:3]]
        session = _StormSession(
            category=category,
            started_at=float(now),
            suppress_until=float(now) + (float(self.suppress_minutes) * 60.0),
            total_count=len(recent),
            hosts=hosts,
            sample_lines=sample_lines,
            notify_host=host,
        )
        self._active_by_category[category] = session
        self._recent_by_category[category] = []
        return {
            "mode": "storm_start",
            "host": host,
            "category": "STORM",
            "message": f"[STORM] {category} 告警风暴：{len(hosts)} 台主机在 {self.window_seconds} 秒内触发 {session.total_count} 条告警\n"
            + "\n".join(sample_lines),
            "payload": {
                "storm_summary": True,
                "storm_phase": "start",
                "category": category,
                "storm_total": session.total_count,
                "storm_hosts": sorted(hosts),
            },
        }
