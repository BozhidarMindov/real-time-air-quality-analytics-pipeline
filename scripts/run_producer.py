import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.common.config import DEFAULT_KAFKA_BOOTSTRAP_SERVERS
from src.common.config import get_default_kafka_topic
from src.common.config import get_required_env
from src.common.logging import configure_logging
from src.ingestion.aqicn_client import AQICNClient
from src.ingestion.producer import DEFAULT_POLL_INTERVAL_SECONDS
from src.ingestion.producer import Producer


DEFAULT_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS = 6
DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = 5


def create_kafka_producer(
    kafka_bootstrap_servers: str,
    logger: logging.Logger,
    retry_attempts: int = DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS,
    retry_backoff_seconds: int = DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS,
    sleep=time.sleep,
):
    """Create the Kafka producer used by the ingestion entrypoint.

    Args:
        kafka_bootstrap_servers: A comma-separated Kafka bootstrap server list.
        logger: The application logger used for retry messages.
        retry_attempts: A number of Kafka connection attempts before failing.
        retry_backoff_seconds: A delay between Kafka retry attempts.
        sleep: The sleep function used between Kafka retry attempts.

    Returns:
        The Kafka producer used by the ingestion entrypoint.

    Raises:
        NoBrokersAvailable: The final Kafka connection error after all retries are exhausted.
    """
    bootstrap_servers = [
        server.strip()
        for server in kafka_bootstrap_servers.split(",")
        if server.strip()
    ]
    for attempt in range(1, retry_attempts + 1):
        try:
            return KafkaProducer(bootstrap_servers=bootstrap_servers)
        except NoBrokersAvailable:
            if attempt >= retry_attempts:
                raise
            logger.warning(
                f"Kafka broker not available on attempt {attempt}/{retry_attempts}. Retrying in {retry_backoff_seconds} seconds"
            )
            sleep(retry_backoff_seconds)


def main(env_path: str | None = None) -> int:
    """Run the producer entrypoint.

    Args:
        env_path: An optional dotenv file path used before creating the producer.

    Returns:
        The process exit code.
    """
    configure_logging()
    logger = logging.getLogger("air_quality.ingestion")
    dotenv_path = Path(env_path) if env_path is not None else DEFAULT_ENV_PATH
    load_dotenv(dotenv_path=dotenv_path)
    api_token = get_required_env("AQICN_API_TOKEN")
    city = get_required_env("CITY")
    kafka_bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    )
    kafka_topic = get_default_kafka_topic(city)

    aqicn_client = AQICNClient(
        api_token=api_token,
        base_url=os.getenv("AQICN_BASE_URL") or "https://api.waqi.info/feed",
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS") or "30"),
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS") or "3"),
        retry_backoff_seconds=int(os.getenv("RETRY_BACKOFF_SECONDS") or "5"),
    )
    kafka_producer = create_kafka_producer(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        logger=logger,
    )

    producer = Producer(
        aqicn_client=aqicn_client,
        kafka_producer=kafka_producer,
        city=city,
        kafka_topic=kafka_topic,
        poll_interval_seconds=int(
            os.getenv("POLL_INTERVAL_SECONDS") or str(DEFAULT_POLL_INTERVAL_SECONDS)
        ),
    )
    producer.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
