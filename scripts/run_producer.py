import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from src.common.logging import configure_logging
from src.ingestion.aqicn_client import AQICNClient
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
    aqicn_client = AQICNClient(
        api_token=os.getenv("AQICN_API_TOKEN") or "",
        base_url=os.getenv("AQICN_BASE_URL") or "https://api.waqi.info/feed",
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS") or "30"),
        retry_attempts=int(os.getenv("RETRY_ATTEMPTS") or "3"),
        retry_backoff_seconds=int(os.getenv("RETRY_BACKOFF_SECONDS") or "5"),
    )
    kafka_producer = KafkaProducer(
        bootstrap_servers=[
            server.strip()
            for server in (
                os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9094"
            ).split(",")
            if server.strip()
        ]
    )

    producer = Producer(
        aqicn_client=aqicn_client,
        kafka_producer=kafka_producer,
        city=os.getenv("CITY") or "sofia",
        kafka_topic=os.getenv("KAFKA_TOPIC") or "air_quality_sofia",
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS") or "60"),
    )
    producer.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
