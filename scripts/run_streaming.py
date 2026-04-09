import logging
import os

from src.streaming.consumer import Consumer
from src.streaming.consumer import DEFAULT_BOOTSTRAP_SERVERS
from src.streaming.consumer import DEFAULT_CITY
from src.streaming.consumer import DEFAULT_KAFKA_TOPIC
from src.streaming.consumer import DEFAULT_LOCAL_STAGING_DIR
from src.streaming.consumer import DEFAULT_OUTPUT_ROOT
from src.streaming.hdfs_client import DEFAULT_HDFS_NAMENODE_URL
from src.streaming.hdfs_client import DEFAULT_HDFS_USER


def _getenv_or_default(key: str, default: str | None) -> str | None:
    """Return an environment value when present or fall back to a default.

    Args:
        key: The environment variable name to read.
        default: The fallback value used when the variable is missing or empty.

    Returns:
        The configured environment value or the fallback.
    """
    value = os.getenv(key)
    return value if value else default


def main() -> int:
    """Run the streaming consumer entrypoint.

    Returns:
        The process exit code.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    consumer = Consumer(
        aqicn_api_token="",
        kafka_bootstrap_servers=_getenv_or_default(
            "KAFKA_BOOTSTRAP_SERVERS",
            DEFAULT_BOOTSTRAP_SERVERS,
        ),
        kafka_topic=_getenv_or_default(
            "KAFKA_TOPIC",
            DEFAULT_KAFKA_TOPIC,
        ),
        city=_getenv_or_default(
            "CITY",
            DEFAULT_CITY,
        ),
        output_root=_getenv_or_default(
            "OUTPUT_ROOT",
            DEFAULT_OUTPUT_ROOT,
        ),
        processing_date=_getenv_or_default("PROCESSING_DATE", None),
        hdfs_namenode_url=_getenv_or_default(
            "HDFS_NAMENODE_URL",
            DEFAULT_HDFS_NAMENODE_URL,
        ),
        hdfs_user=_getenv_or_default(
            "HDFS_USER",
            DEFAULT_HDFS_USER,
        ),
        local_staging_dir=_getenv_or_default(
            "LOCAL_STAGING_DIR",
            DEFAULT_LOCAL_STAGING_DIR,
        ),
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        logging.getLogger("air_quality.streaming").info("Streaming consumer stopped")
    return 0


def run_cli() -> int:
    """Run the streaming command-line wrapper.

    Returns:
        The process exit code.
    """
    return main()


if __name__ == "__main__":
    raise SystemExit(run_cli())
