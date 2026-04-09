from pathlib import Path

from dotenv import load_dotenv

from src.common.env import get_env_or_default
from src.common.logging import configure_logging
from src.ingestion.producer import Producer


DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"

def main(env_path: str | None = None) -> int:
    """Run the producer entrypoint.

    Args:
        env_path: An optional dotenv file path used before creating the producer.

    Returns:
        The process exit code.
    """
    configure_logging()
    dotenv_path = Path(env_path) if env_path is not None else DEFAULT_ENV_PATH
    load_dotenv(dotenv_path=dotenv_path)

    producer = Producer(
        aqicn_api_token=get_env_or_default("AQICN_API_TOKEN", ""),
        city=get_env_or_default("CITY", "sofia"),
        poll_interval_seconds=int(get_env_or_default("POLL_INTERVAL_SECONDS", "60")),
        kafka_bootstrap_servers=get_env_or_default("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        kafka_topic=get_env_or_default("KAFKA_TOPIC", "air_quality_sofia"),
        aqicn_base_url=get_env_or_default("AQICN_BASE_URL", "https://api.waqi.info/feed"),
        request_timeout_seconds=int(get_env_or_default("REQUEST_TIMEOUT_SECONDS", "30")),
        retry_attempts=int(get_env_or_default("RETRY_ATTEMPTS", "3")),
        retry_backoff_seconds=int(get_env_or_default("RETRY_BACKOFF_SECONDS", "5")),
    )
    producer.run()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
