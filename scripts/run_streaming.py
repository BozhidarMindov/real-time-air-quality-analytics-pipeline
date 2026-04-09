import logging

from src.common.env import get_env_or_default
from src.common.logging import configure_logging
from src.streaming.consumer import Consumer
from src.streaming.consumer import DEFAULT_BOOTSTRAP_SERVERS
from src.streaming.consumer import DEFAULT_CITY
from src.streaming.consumer import DEFAULT_KAFKA_TOPIC
from src.streaming.consumer import DEFAULT_LOCAL_STAGING_DIR
from src.streaming.consumer import DEFAULT_OUTPUT_ROOT
from src.streaming.hdfs_client import DEFAULT_HDFS_NAMENODE_URL
from src.streaming.hdfs_client import DEFAULT_HDFS_USER

def main() -> int:
    """Run the streaming consumer entrypoint.

    Returns:
        The process exit code.
    """
    configure_logging()
    consumer = Consumer(
        aqicn_api_token="",
        kafka_bootstrap_servers=get_env_or_default(
            "KAFKA_BOOTSTRAP_SERVERS",
            DEFAULT_BOOTSTRAP_SERVERS,
        ),
        kafka_topic=get_env_or_default(
            "KAFKA_TOPIC",
            DEFAULT_KAFKA_TOPIC,
        ),
        city=get_env_or_default(
            "CITY",
            DEFAULT_CITY,
        ),
        output_root=get_env_or_default(
            "OUTPUT_ROOT",
            DEFAULT_OUTPUT_ROOT,
        ),
        processing_date=get_env_or_default("PROCESSING_DATE", None),
        hdfs_namenode_url=get_env_or_default(
            "HDFS_NAMENODE_URL",
            DEFAULT_HDFS_NAMENODE_URL,
        ),
        hdfs_user=get_env_or_default(
            "HDFS_USER",
            DEFAULT_HDFS_USER,
        ),
        local_staging_dir=get_env_or_default(
            "LOCAL_STAGING_DIR",
            DEFAULT_LOCAL_STAGING_DIR,
        ),
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        logging.getLogger("air_quality.streaming").info("Streaming consumer stopped")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
