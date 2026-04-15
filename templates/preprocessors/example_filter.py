"""
Custom Preprocessor Template
=============================

Copy this file to templates/preprocessors/<your_name>.py and implement your
log filtering or transformation logic in the process() method.

Reference by name in your host config (logdog.yaml):

    hosts:
      - name: prod
        preprocessors:
          - name: example_filter       # matches filename without .py
            ignore_paths: ["/health", "/ping"]
          - name: dedup                # built-in, can mix freely
            max_consecutive: 3
        prompt_template: alert_brief

The process() method receives ALL log lines for the host as a list of LogLine
objects and must return a list (same type). You can:
  - Filter: return fewer lines (drop noise)
  - Transform: modify line.content or line.level
  - Enrich: add data to line.metadata

Each LogLine has:
    host_name      str         — host this line came from
    container_id   str         — Docker container ID (short hash)
    container_name str         — Docker container name
    timestamp      str         — ISO 8601 timestamp string
    content        str         — raw log line text
    level          str | None  — detected log level (may be None)
    metadata       dict | None — arbitrary key/value bag

Important: return a NEW list; do not mutate the input list in place.
"""
from __future__ import annotations

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


class ExampleFilterPreprocessor(BasePreprocessor):
    # name must match the filename (without .py) for config lookup to work
    name = "example_filter"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        # Read config values passed from YAML, e.g.:
        #   preprocessors:
        #     - name: example_filter
        #       ignore_paths: ["/health", "/ping"]
        self._ignore_paths: list[str] = list(cfg.get("ignore_paths", []))

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        result = []
        for line in lines:
            # --- Example 1: drop lines containing specific path strings ---
            if any(path in line.content for path in self._ignore_paths):
                continue

            # --- Example 2: container-specific logic ---
            # LogLine carries container_name, so one script can handle
            # multiple containers differently.
            if line.container_name == "nginx":
                # Drop nginx access logs for loopback health checks
                if line.content.startswith("127.0.0.1") and "GET /health" in line.content:
                    continue

            # --- Example 3: enrich metadata for downstream use ---
            if "CRITICAL" in line.content and line.level is None:
                line = LogLine(
                    host_name=line.host_name,
                    container_id=line.container_id,
                    container_name=line.container_name,
                    timestamp=line.timestamp,
                    content=line.content,
                    level="critical",
                    metadata={**(line.metadata or {}), "enriched_by": "example_filter"},
                )

            result.append(line)
        return result


# Required export — loader looks for PREPROCESSOR (class or instance both work)
PREPROCESSOR = ExampleFilterPreprocessor
