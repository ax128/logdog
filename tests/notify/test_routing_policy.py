from __future__ import annotations

from logwatch.notify.policy import build_notify_routing_policy


def test_notify_policy_matches_container_specific_rule_then_fallback() -> None:
    policy = build_notify_routing_policy(
        {
            "default_channels": ["telegram"],
            "rules": [
                {
                    "name": "api-critical",
                    "priority": 100,
                    "match": {
                        "hosts": ["prod-*"],
                        "containers": ["api", "gateway-*"],
                        "categories": ["ERROR", "OOM"],
                    },
                    "deliver": {
                        "channels": ["wechat"],
                        "message_mode": "md",
                    },
                }
            ],
        }
    )

    matched = policy.resolve(
        host="prod-a",
        category="ERROR",
        context={"container_name": "api"},
    )
    fallback = policy.resolve(
        host="prod-a",
        category="ERROR",
        context={"container_name": "worker"},
    )

    assert matched["channels"] == ["wechat"]
    assert matched["message_mode"] == "md"
    assert fallback["channels"] == ["telegram"]
