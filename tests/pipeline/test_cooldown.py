from logwatch.pipeline.cooldown import CooldownStore


def test_cooldown_blocks_same_host_container_category_within_window():
    cd = CooldownStore(default_minutes=1)
    assert cd.allow("h1", "c1", "ERROR", now=0) is True
    assert cd.allow("h1", "c1", "ERROR", now=10) is False


def test_cooldown_different_category_not_interfere():
    cd = CooldownStore(default_minutes=1)
    assert cd.allow("h1", "c1", "ERROR", now=0) is True
    assert cd.allow("h1", "c1", "OOM", now=10) is True


def test_cooldown_expires_then_allows_again():
    cd = CooldownStore(default_minutes=1)
    assert cd.allow("h1", "c1", "ERROR", now=0) is True
    assert cd.allow("h1", "c1", "ERROR", now=30) is False
    assert cd.allow("h1", "c1", "ERROR", now=61) is True


def test_cooldown_per_category_overrides_default_window():
    cd = CooldownStore(default_minutes=10, per_category={"ERROR": 0.1})  # 6 seconds
    assert cd.allow("h1", "c1", "ERROR", now=0) is True
    assert cd.allow("h1", "c1", "ERROR", now=5) is False
    assert cd.allow("h1", "c1", "ERROR", now=7) is True


def test_cooldown_clock_rollback_resets_window():
    cd = CooldownStore(default_minutes=1)
    assert cd.allow("h1", "c1", "ERROR", now=100.0) is True
    assert cd.allow("h1", "c1", "ERROR", now=99.0) is True
    assert cd.allow("h1", "c1", "ERROR", now=100.0) is False
    assert cd.allow("h1", "c1", "ERROR", now=160.0) is True
