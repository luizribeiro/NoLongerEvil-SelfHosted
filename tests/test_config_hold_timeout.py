"""Tests for the configurable subscribe connection-hold timeout."""

from nolongerevil.config.environment import Settings


def test_hold_timeout_defaults_to_suspend_minus_ten() -> None:
    s = Settings(suspend_time_max=300, connection_hold_timeout_seconds=None)
    assert s.connection_hold_timeout == 290.0


def test_hold_timeout_override_takes_precedence() -> None:
    s = Settings(suspend_time_max=300, connection_hold_timeout_seconds=60)
    assert s.connection_hold_timeout == 60.0


def test_hold_timeout_reads_env_var(monkeypatch) -> None:
    monkeypatch.setenv("CONNECTION_HOLD_TIMEOUT_SECONDS", "45")
    s = Settings()
    assert s.connection_hold_timeout == 45.0
