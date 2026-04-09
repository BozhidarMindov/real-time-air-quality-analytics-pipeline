from src.common.env import get_env_or_default
from src.common.env import get_int_env_or_default


def test_get_env_or_default_returns_existing_value(mocker):
    mocker.patch("src.common.env.os.getenv", return_value="varna")

    result = get_env_or_default("CITY", "sofia")

    assert result == "varna"


def test_get_env_or_default_returns_default_for_blank_value(mocker):
    mocker.patch("src.common.env.os.getenv", return_value="")

    result = get_env_or_default("CITY", "sofia")

    assert result == "sofia"


def test_get_int_env_or_default_returns_integer_value(mocker):
    mocker.patch("src.common.env.os.getenv", return_value="120")

    result = get_int_env_or_default("POLL_INTERVAL_SECONDS", 60)

    assert result == 120
