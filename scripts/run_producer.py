import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from src.ingestion.producer import Producer


DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _getenv_or_default(key: str, default: str) -> str:
    """Return an environment value when present or fall back to a default.

    Args:
        key: The environment variable name to read.
        default: The fallback value used when the variable is missing or empty.

    Returns:
        The configured environment value or the fallback.
    """
    value = os.getenv(key)
    return value if value else default


def main(env_path: str | None = None) -> int:
    """Run the producer entrypoint.

    Args:
        env_path: An optional dotenv file path used before creating the producer.

    Returns:
        The process exit code.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    dotenv_path = Path(env_path) if env_path is not None else DEFAULT_ENV_PATH
    load_dotenv(dotenv_path=dotenv_path)

    producer = Producer(
        aqicn_api_token=_getenv_or_default("AQICN_API_TOKEN", ""),
        city=_getenv_or_default("CITY", "sofia"),
        poll_interval_seconds=int(_getenv_or_default("POLL_INTERVAL_SECONDS", "60")),
        kafka_bootstrap_servers=_getenv_or_default("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        kafka_topic=_getenv_or_default("KAFKA_TOPIC", "air_quality_sofia"),
        aqicn_base_url=_getenv_or_default("AQICN_BASE_URL", "https://api.waqi.info/feed"),
        request_timeout_seconds=int(_getenv_or_default("REQUEST_TIMEOUT_SECONDS", "30")),
        retry_attempts=int(_getenv_or_default("RETRY_ATTEMPTS", "3")),
        retry_backoff_seconds=int(_getenv_or_default("RETRY_BACKOFF_SECONDS", "5")),
    )
    producer.run()
    return 0


def run_cli() -> int:
    """Run the producer command-line wrapper.

    Returns:
        The process exit code.
    """
    return main()

if __name__ == "__main__":
    raise SystemExit(run_cli())
