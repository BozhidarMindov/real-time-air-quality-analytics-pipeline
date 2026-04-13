import logging
import os
import tempfile
import time
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from src.common.logging import configure_logging
from src.streaming.consumer import Consumer
from src.streaming.hdfs_client import DEFAULT_HDFS_NAMENODE_URL
from src.streaming.hdfs_client import DEFAULT_HDFS_USER
from src.streaming.hdfs_client import HDFSClient


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9094"
DEFAULT_KAFKA_TOPIC = "air_quality_sofia"
DEFAULT_CITY = "sofia"
DEFAULT_OUTPUT_ROOT = "/data/air-quality"
DEFAULT_LOCAL_STAGING_DIR = str(Path(tempfile.gettempdir()) / "air-quality")
DEFAULT_CONSUMER_GROUP = "air-quality-streaming"
DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS = 6
DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = 5
IDENTITY_DESERIALIZER = lambda value: value


def create_kafka_consumer(
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    logger: logging.Logger,
    consumer_group: str = DEFAULT_CONSUMER_GROUP,
    retry_attempts: int = DEFAULT_KAFKA_CONNECT_RETRY_ATTEMPTS,
    retry_backoff_seconds: int = DEFAULT_KAFKA_CONNECT_RETRY_BACKOFF_SECONDS,
    sleep=time.sleep,
):
    """Create the Kafka consumer used by the streaming entrypoint.

    Args:
        kafka_bootstrap_servers: A comma-separated Kafka bootstrap server list.
        kafka_topic: A Kafka topic name.
        logger: The application logger used for retry messages.
        consumer_group: A Kafka consumer group id.
        retry_attempts: A number of Kafka connection attempts before failing.
        retry_backoff_seconds: A delay between Kafka connection attempts.
        sleep: A sleep function used between Kafka retry attempts.

    Returns:
        The Kafka consumer used by the streaming entrypoint.

    Raises:
        NoBrokersAvailable: The final Kafka connection error after all retries are exhausted.
    """
    bootstrap_servers = [
        server.strip()
        for server in kafka_bootstrap_servers.split(",")
        if server.strip()
    ]
    last_error = None

    for attempt in range(1, retry_attempts + 1):
        try:
            return KafkaConsumer(
                kafka_topic,
                bootstrap_servers=bootstrap_servers,
                group_id=consumer_group,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=IDENTITY_DESERIALIZER,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            if attempt >= retry_attempts:
                break
            logger.warning(
                f"Kafka broker not available on attempt {attempt}/{retry_attempts}; "
                f"retrying in {retry_backoff_seconds} seconds"
            )
            sleep(retry_backoff_seconds)

    assert last_error is not None
    raise last_error


def main() -> int:
    """Run the streaming consumer entrypoint.

    Returns:
        The process exit code.
    """
    configure_logging()
    logger = logging.getLogger("air_quality.streaming")
    kafka_bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_BOOTSTRAP_SERVERS
    )
    kafka_topic = os.getenv("KAFKA_TOPIC") or DEFAULT_KAFKA_TOPIC
    city = os.getenv("CITY") or DEFAULT_CITY
    output_root = os.getenv("OUTPUT_ROOT") or DEFAULT_OUTPUT_ROOT
    processing_date = os.getenv("PROCESSING_DATE") or None
    hdfs_namenode_url = os.getenv("HDFS_NAMENODE_URL") or DEFAULT_HDFS_NAMENODE_URL
    hdfs_user = os.getenv("HDFS_USER") or DEFAULT_HDFS_USER
    local_staging_dir = os.getenv("LOCAL_STAGING_DIR") or DEFAULT_LOCAL_STAGING_DIR
    kafka_consumer = create_kafka_consumer(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        logger=logger,
    )
    hdfs_client = HDFSClient(
        namenode_url=hdfs_namenode_url,
        user=hdfs_user,
    )
    consumer = Consumer(
        kafka_consumer=kafka_consumer,
        hdfs_client=hdfs_client,
        city=city,
        output_root=output_root,
        processing_date=processing_date,
        local_staging_dir=local_staging_dir,
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Streaming consumer stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
