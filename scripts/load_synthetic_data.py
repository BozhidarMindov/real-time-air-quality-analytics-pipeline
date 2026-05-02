import json
import logging
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from src.common.config import DEFAULT_KAFKA_BOOTSTRAP_SERVERS
from src.common.config import get_default_kafka_topic
from src.common.config import get_required_env
from src.common.logging import configure_logging
from src.synthetic.generator import generate_synthetic_payloads


DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
DEFAULT_SYNTHETIC_DAYS = 15
DEFAULT_SYNTHETIC_INTERVAL_MINUTES = 60
DEFAULT_SYNTHETIC_STATION_COUNT = 1


def utc_now() -> datetime:
    """Return the current UTC timestamp.

    Returns:
        The current timezone-aware UTC datetime.
    """
    return datetime.now(timezone.utc)


def main(env_path: str | None = None) -> int:
    """Publish synthetic AQICN-shaped payloads to the city Kafka topic.

    Args:
        env_path: An optional dotenv file path loaded before reading settings.

    Returns:
        The process exit code.
    """
    configure_logging()
    logger = logging.getLogger("air_quality.synthetic")
    dotenv_path = Path(env_path) if env_path is not None else DEFAULT_ENV_PATH
    load_dotenv(dotenv_path=dotenv_path)

    city = get_required_env("CITY")
    kafka_bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    )
    days = int(os.getenv("SYNTHETIC_DAYS") or str(DEFAULT_SYNTHETIC_DAYS))
    interval_minutes = int(
        os.getenv("SYNTHETIC_INTERVAL_MINUTES")
        or str(DEFAULT_SYNTHETIC_INTERVAL_MINUTES)
    )
    station_count = int(
        os.getenv("SYNTHETIC_STATION_COUNT") or str(DEFAULT_SYNTHETIC_STATION_COUNT)
    )
    kafka_topic = get_default_kafka_topic(city)
    bootstrap_servers = [
        server.strip()
        for server in kafka_bootstrap_servers.split(",")
        if server.strip()
    ]

    start_at = utc_now().replace(minute=0, second=0, microsecond=0) - timedelta(
        days=days
    )
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    sent_count = 0

    try:
        for payload in generate_synthetic_payloads(
            city=city,
            start_at=start_at,
            days=days,
            interval_minutes=interval_minutes,
            station_count=station_count,
        ):
            producer.send(
                kafka_topic,
                value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            )
            sent_count += 1
        producer.flush()
    finally:
        producer.close()

    logger.info(f"Published {sent_count} synthetic payloads to {kafka_topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
