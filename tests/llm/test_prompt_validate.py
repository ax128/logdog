from logwatch.llm.prompts.loader import load_template


def test_heartbeat_scene_does_not_require_logs():
    tmpl = load_template("default_alert")

    required = tmpl.required_vars("heartbeat")
    assert "logs" not in required
    assert required == {
        "host_name",
        "timestamp",
        "container_status",
        "total_containers",
    }

    missing = tmpl.validate(
        "heartbeat",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "container_status": "api=ok",
            "total_containers": 4,
        },
    )
    assert missing == []


def test_alert_scene_requires_spec_fields():
    tmpl = load_template("default_alert")

    assert tmpl.required_vars("alert") == {
        "host_name",
        "container_name",
        "timestamp",
        "logs",
    }


def test_non_alert_scenes_allow_empty_collections_for_required_data_fields():
    tmpl = load_template("default_alert")

    interval_missing = tmpl.validate(
        "interval",
        {
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": [],
            "metrics": {},
        },
    )
    daily_missing = tmpl.validate(
        "daily",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "alert_history": [],
            "metrics": {},
        },
    )
    heartbeat_missing = tmpl.validate(
        "heartbeat",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "container_status": [],
            "total_containers": 0,
        },
    )

    assert interval_missing == []
    assert daily_missing == []
    assert heartbeat_missing == []
