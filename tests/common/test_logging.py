import logging

from src.common.logging import configure_logging


def test_configure_logging_uses_shared_level_and_format(mocker):
    basic_config = mocker.patch("src.common.logging.logging.basicConfig")

    configure_logging()

    basic_config.assert_called_once_with(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
