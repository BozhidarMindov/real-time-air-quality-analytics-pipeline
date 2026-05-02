import pytest

from src.common.config import DEFAULT_KAFKA_BOOTSTRAP_SERVERS
from src.common.config import get_default_kafka_topic
from src.common.config import get_required_env


def test_get_required_env_returns_configured_value(monkeypatch):
    monkeypatch.setenv("CITY", "varna")

    result = get_required_env("CITY")

    assert result == "varna"


@pytest.mark.parametrize("missing_value", [None, ""])
def test_get_required_env_raises_for_missing_or_blank_value(monkeypatch, missing_value):
    if missing_value is None:
        monkeypatch.delenv("CITY", raising=False)
    else:
        monkeypatch.setenv("CITY", missing_value)

    with pytest.raises(ValueError, match="CITY is required"):
        get_required_env("CITY")


def test_get_default_kafka_topic_uses_city_name():
    result = get_default_kafka_topic("varna")

    assert result == "air_quality_varna"


def test_default_kafka_bootstrap_servers_uses_localhost_listener():
    assert DEFAULT_KAFKA_BOOTSTRAP_SERVERS == "localhost:9094"
